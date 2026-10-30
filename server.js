
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const { spawn } = require("child_process");
const { pipeline } = require("stream/promises");
const multer = require("multer");
dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;
const MAX_BYTES = 100 * 1024 * 1024; // 100 MB
const FILE_TTL_MS = 15 * 60 * 1000; // 15 minutes

const allowedHosts = new Set(
  (process.env.ALLOWED_MEDIA_HOSTS || "")
    .split(",")
    .map(host => host.trim().toLowerCase())
    .filter(Boolean)
);

const frontendOrigin =
  process.env.FRONTEND_ORIGIN || "https://ytmp4mp3.kesug.com";

app.use(cors({ origin: frontendOrigin }));
app.use(express.json({ limit: "10kb" }));

const jobs = new Map();
const workRoot = path.join(os.tmpdir(), "media-converter");

fs.mkdirSync(workRoot, { recursive: true });

const uploadStorage = multer.diskStorage({
  destination: (req, file, callback) => {
    callback(null, workRoot);
  },
  filename: (req, file, callback) => {
    callback(null, `upload-${crypto.randomUUID()}`);
  }
});

const upload = multer({
  storage: uploadStorage,
  limits: {
    fileSize: MAX_BYTES,
    files: 1
  },
  fileFilter: (req, file, callback) => {
    const type = (file.mimetype || "").toLowerCase();

    if (
      type.startsWith("audio/") ||
      type.startsWith("video/") ||
      type === "application/octet-stream"
    ) {
      callback(null, true);
    } else {
      callback(new Error("Please upload an audio or video file."));
    }
  }
});

app.get("/api/health", (req, res) => {
  res.json({ status: "ok" });
});

app.get("/api/ffmpeg-check", (req, res) => {
  const { execFile } = require("child_process");

  execFile("ffmpeg", ["-version"], (error, stdout) => {
    if (error) {
      return res.status(500).json({
        installed: false,
        message: "FFmpeg is not installed or not available."
      });
    }

    res.json({
      installed: true,
      version: stdout.split("\n")[0]
    });
  });
});

function validateUrl(value) {
  let url;

  try {
    url = new URL(value);
  } catch {
    throw new Error("Please enter a valid direct media URL.");
  }

  if (url.protocol !== "https:") {
    throw new Error("Only HTTPS media URLs are allowed.");
  }

  if (url.username || url.password) {
    throw new Error("URLs containing login credentials are not allowed.");
  }

  if (url.port && url.port !== "443") {
    throw new Error("Custom URL ports are not allowed.");
  }

  if (!allowedHosts.has(url.hostname.toLowerCase())) {
    throw new Error(
      "This media domain is not allowlisted by the server owner."
    );
  }

  return url;
}

async function downloadMedia(startUrl, outputPath) {
  let url = validateUrl(startUrl);
  let response;

  // Re-check every redirect destination.
  for (let i = 0; i < 4; i++) {
    response = await fetch(url, {
      redirect: "manual",
      signal: AbortSignal.timeout(30000)
    });

    if ([301, 302, 303, 307, 308].includes(response.status)) {
      const location = response.headers.get("location");

      if (!location) {
        throw new Error("Invalid redirect from media server.");
      }

      await response.body?.cancel();
      url = validateUrl(new URL(location, url).href);
      continue;
    }

    break;
  }

  if (!response || !response.ok) {
    throw new Error("Could not fetch media from that URL.");
  }

  const type = (
    response.headers.get("content-type") || ""
  ).split(";")[0].trim().toLowerCase();

  if (
    !type.startsWith("audio/") &&
    !type.startsWith("video/") &&
    type !== "application/octet-stream"
  ) {
    await response.body?.cancel();
    throw new Error("The URL does not appear to serve a media file.");
  }

  const declaredSize = Number(
    response.headers.get("content-length") || 0
  );

  if (declaredSize > MAX_BYTES) {
    await response.body?.cancel();
    throw new Error("Maximum input size is 100 MB.");
  }

  if (!response.body) {
    throw new Error("The media server returned an empty response.");
  }

  let bytes = 0;
  const { Transform } = require("stream");

  const limiter = new Transform({
    transform(chunk, encoding, callback) {
      bytes += chunk.length;

      if (bytes > MAX_BYTES) {
        callback(new Error("Maximum input size is 100 MB."));
        return;
      }

      callback(null, chunk);
    }
  });

  await pipeline(
    require("stream").Readable.fromWeb(response.body),
    limiter,
    fs.createWriteStream(outputPath, { flags: "wx" })
  );

  if (bytes === 0) {
    throw new Error("The downloaded media file is empty.");
  }
}

function runFfmpeg(inputPath, outputPath, format) {
  return new Promise((resolve, reject) => {
    const args = [
      "-nostdin",
      "-hide_banner",
      "-loglevel", "error",
      "-y",
      "-i", inputPath,
      "-map", "0:a:0?"
    ];

    if (format === "mp3") {
      args.push(
        "-vn",
        "-codec:a", "libmp3lame",
        "-q:a", "2"
      );
    } else {
      args.push(
        "-map", "0:v:0",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart"
      );
    }

    args.push("-t", "600", outputPath);

    const child = spawn("ffmpeg", args, {
      stdio: ["ignore", "ignore", "pipe"]
    });

    let errorText = "";

    child.stderr.on("data", data => {
      errorText += data.toString();

      if (errorText.length > 5000) {
        errorText = errorText.slice(-5000);
      }
    });

    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
    }, 120000);

    child.on("error", error => {
      clearTimeout(timeout);
      reject(error);
    });

    child.on("close", code => {
      clearTimeout(timeout);

      if (code === 0 && fs.existsSync(outputPath)) {
        resolve();
      } else {
        reject(
          new Error(
            "Conversion failed. Check that the source contains the required audio or video stream."
          )
        );
      }
    });
  });
}

app.post("/api/upload-convert", (req, res) => {
  upload.single("media")(req, res, async (uploadError) => {
    let jobDir;

    try {
      if (uploadError) {
        throw uploadError;
      }

      if (!req.file) {
        return res.status(400).json({
          success: false,
          error: "Please upload an audio or video file."
        });
      }

      const format = req.body.format;

      if (!["mp3", "mp4"].includes(format)) {
        fs.rmSync(req.file.path, { force: true });

        return res.status(400).json({
          success: false,
          error: "Choose MP3 or MP4."
        });
      }

      const id = crypto.randomUUID();
      jobDir = path.join(workRoot, id);
      fs.mkdirSync(jobDir, { recursive: true });

      const inputPath = path.join(jobDir, "input.media");
      const outputPath = path.join(jobDir, `converted.${format}`);

      fs.renameSync(req.file.path, inputPath);

      await runFfmpeg(inputPath, outputPath, format);

      fs.rmSync(inputPath, { force: true });

      jobs.set(id, {
        path: outputPath,
        format,
        expires: Date.now() + FILE_TTL_MS
      });

      return res.json({
        success: true,
        format,
        downloadUrl: `/api/download/${id}`,
        expiresInSeconds: FILE_TTL_MS / 1000
      });

    } catch (error) {
      if (req.file?.path) {
        fs.rmSync(req.file.path, { force: true });
      }

      if (jobDir) {
        fs.rmSync(jobDir, { recursive: true, force: true });
      }

      return res.status(400).json({
        success: false,
        error: error.message || "Upload conversion failed."
      });
    }
  });
});

app.post("/api/convert", async (req, res) => {
  let jobDir;

  try {
    const { url, format } = req.body || {};

    if (typeof url !== "string" || url.length > 2048) {
      return res.status(400).json({
        error: "Please provide a valid media URL."
      });
    }

    if (!["mp3", "mp4"].includes(format)) {
      return res.status(400).json({
        error: "Choose MP3 or MP4."
      });
    }

    validateUrl(url);

    const id = crypto.randomUUID();
    jobDir = path.join(workRoot, id);
    fs.mkdirSync(jobDir, { recursive: true });

    const inputPath = path.join(jobDir, "input.media");
    const outputPath = path.join(jobDir, `converted.${format}`);

    await downloadMedia(url, inputPath);
    await runFfmpeg(inputPath, outputPath, format);

    fs.rmSync(inputPath, { force: true });

    jobs.set(id, {
      path: outputPath,
      format,
      expires: Date.now() + FILE_TTL_MS
    });

    res.json({
      success: true,
      format,
      downloadUrl: `/api/download/${id}`,
      expiresInSeconds: FILE_TTL_MS / 1000
    });
  } catch (error) {
    if (jobDir) {
      fs.rmSync(jobDir, { recursive: true, force: true });
    }

    res.status(400).json({
      error: error.message || "Conversion failed."
    });
  }
});

app.get("/api/download/:id", (req, res) => {
  const job = jobs.get(req.params.id);

  if (!job || job.expires < Date.now() || !fs.existsSync(job.path)) {
    jobs.delete(req.params.id);

    return res.status(404).json({
      error: "File expired or was not found. Please convert it again."
    });
  }

  res.download(job.path, `converted.${job.format}`, error => {
    if (error) {
      console.error("Download response failed:", error.message);
    }
  });
});

// Remove expired output files.
setInterval(() => {
  for (const [id, job] of jobs) {
    if (job.expires < Date.now()) {
      fs.rmSync(path.dirname(job.path), {
        recursive: true,
        force: true
      });
      jobs.delete(id);
    }
  }
}, 60 * 1000).unref();
const { execFile } = require("child_process");

app.get("/api/ffmpeg-check", (req, res) => {
  execFile("ffmpeg", ["-version"], (error, stdout) => {
    if (error) {
      return res.status(500).json({
        status: "error",
        message: "FFmpeg not found"
      });
    }

    res.json({
      status: "ok",
      ffmpeg: stdout.split("\n")[0]
    });
  });
});
app.listen(PORT, () => {
  console.log(`Media conversion API listening on port ${PORT}`);
});
