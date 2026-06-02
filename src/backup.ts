import { v2 as cloudinary } from "cloudinary";
import { createReadStream, createWriteStream } from "fs";
import { stat, writeFile, mkdir, rm } from "fs/promises";
import { createGzip } from "zlib";
import { pipeline } from "stream/promises";
import { createHash } from "crypto";
import mysqldump from "mysqldump";
import env from "./config";

// 9MB chunk size (under Cloudinary's 10MB limit)
const CHUNK_SIZE = 9 * 1024 * 1024;
const MAX_UPLOAD_RETRIES = 3;
const RETRY_DELAY_MS = 2000;

cloudinary.config({
  cloud_name: env.CLOUDINARY.CLOUD_NAME,
  api_key: env.CLOUDINARY.API_KEY,
  api_secret: env.CLOUDINARY.API_SECRET,
});

interface Manifest {
  originalFilename: string;
  totalParts: number;
  totalSize: number;
  createdAt: string;
  parts: {
    filename: string;
    size: number;
    checksum: string;
  }[];
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const calculateChecksum = async (filepath: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    const hash = createHash("md5");
    const stream = createReadStream(filepath);
    stream.on("data", (data) => hash.update(data));
    stream.on("end", () => resolve(hash.digest("hex")));
    stream.on("error", reject);
  });
};

const splitFile = async (
  inputPath: string,
  outputDir: string,
  baseFilename: string
): Promise<{ parts: string[]; sizes: number[] }> => {
  console.log("Splitting backup into parts...");

  const fileStats = await stat(inputPath);
  const totalSize = fileStats.size;
  const totalParts = Math.ceil(totalSize / CHUNK_SIZE);

  console.log(`Total size: ${(totalSize / 1024 / 1024).toFixed(2)}MB, splitting into ${totalParts} parts...`);

  const parts: string[] = [];
  const sizes: number[] = [];

  const inputStream = createReadStream(inputPath, { highWaterMark: CHUNK_SIZE });
  let partNumber = 1;

  for await (const chunk of inputStream) {
    const partFilename = `${baseFilename}.${String(partNumber).padStart(3, "0")}`;
    const partPath = `${outputDir}/${partFilename}`;

    await writeFile(partPath, chunk);
    parts.push(partPath);
    sizes.push(chunk.length);

    console.log(`Created part ${partNumber}/${totalParts}: ${partFilename} (${(chunk.length / 1024 / 1024).toFixed(2)}MB)`);
    partNumber++;
  }

  console.log(`Split complete: ${parts.length} parts created`);
  return { parts, sizes };
};

const uploadToCloudinaryWithRetry = async ({
  name,
  path,
  folder,
}: {
  name: string;
  path: string;
  folder: string;
}): Promise<string> => {
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= MAX_UPLOAD_RETRIES; attempt++) {
    try {
      const result = await cloudinary.uploader.upload(path, {
        public_id: name,
        resource_type: "raw",
        folder,
      });
      return result.secure_url;
    } catch (error) {
      lastError = error as Error;
      console.log(`Upload attempt ${attempt}/${MAX_UPLOAD_RETRIES} failed: ${lastError.message}`);

      if (attempt < MAX_UPLOAD_RETRIES) {
        console.log(`Retrying in ${RETRY_DELAY_MS / 1000} seconds...`);
        await sleep(RETRY_DELAY_MS);
      }
    }
  }

  throw new Error(`Failed to upload after ${MAX_UPLOAD_RETRIES} attempts: ${lastError?.message}`);
};

const deleteCloudinaryFiles = async (folder: string, fileNames: string[]) => {
  console.log("Rolling back: deleting uploaded files from Cloudinary...");
  for (const fileName of fileNames) {
    try {
      const publicId = `${folder}/${fileName}`;
      await cloudinary.uploader.destroy(publicId, { resource_type: "raw" });
      console.log(`Deleted: ${publicId}`);
    } catch (error) {
      console.log(`Warning: Could not delete ${fileName} from Cloudinary`);
    }
  }
};

const dumpDatabase = async (path: string) => {
  console.log("Dumping database...");

  await mysqldump({
    connection: {
      host: env.DATABASE.MYSQL_HOST!,
      user: env.DATABASE.MYSQL_USERNAME!,
      password: env.DATABASE.MYSQL_PASSWORD!,
      database: env.DATABASE.MYSQL_DATABASE!,
      port: Number(env.DATABASE.MYSQL_PORT),
    },
    dumpToFile: path,
  });
  

  
  
  
  console.log("Database dumped successfully...");
};

const compressFile = async (inputPath: string, outputPath: string) => {
  console.log("Compressing backup...");

  const source = createReadStream(inputPath);
  const destination = createWriteStream(outputPath);
  const gzip = createGzip({ level: 9 }); // Maximum compression

  await pipeline(source, gzip, destination);

  console.log("Backup compressed successfully...");
};

const cleanupTempDir = async (tempDir: string) => {
  console.log("Cleaning up temporary files...");
  try {
    await rm(tempDir, { recursive: true, force: true });
  } catch {
    // Ignore cleanup errors
  }
};

export const backup = async () => {
  console.log("Initiating DB backup...");

  const date = new Date();
  const timestamp = date.toISOString().replace(/[:.]+/g, "-");
  const backupFolder = `databaseBackups/${date.getFullYear()}/Month-${date.getMonth() + 1}/backup-${timestamp}`;

  const filename = `backup-${timestamp}.sql`;
  const compressedFilename = `${filename}.gz`;
  const tempDir = `/tmp/backup-${timestamp}`;
  const filepath = `${tempDir}/${filename}`;
  const compressedFilepath = `${tempDir}/${compressedFilename}`;

  const uploadedFiles: string[] = [];

  try {
    // Create temp directory
    await mkdir(tempDir, { recursive: true });

    // Step 1: Dump database
    await dumpDatabase(filepath);

    // Step 2: Compress
    await compressFile(filepath, compressedFilepath);

    // Check compressed file size
    const compressedStats = await stat(compressedFilepath);
    const needsSplit = compressedStats.size > CHUNK_SIZE;

    let manifest: Manifest;
    let filesToUpload: { path: string; name: string }[] = [];

    if (needsSplit) {
      // Step 3a: Split into parts
      const { parts, sizes } = await splitFile(compressedFilepath, tempDir, compressedFilename);

      // Step 4: Calculate checksums and create manifest
      console.log("Calculating checksums...");
      manifest = {
        originalFilename: compressedFilename,
        totalParts: parts.length,
        totalSize: sizes.reduce((a, b) => a + b, 0),
        createdAt: date.toISOString(),
        parts: [],
      };

      for (let i = 0; i < parts.length; i++) {
        const partPath = parts[i];
        const partFilename = partPath.split("/").pop()!;
        const checksum = await calculateChecksum(partPath);
        manifest.parts.push({
          filename: partFilename,
          size: sizes[i],
          checksum,
        });
        filesToUpload.push({ path: partPath, name: partFilename });
      }
    } else {
      // Step 3b: No split needed, upload single file
      console.log(`File size (${(compressedStats.size / 1024 / 1024).toFixed(2)}MB) is under limit, no split needed`);

      const checksum = await calculateChecksum(compressedFilepath);
      manifest = {
        originalFilename: compressedFilename,
        totalParts: 1,
        totalSize: compressedStats.size,
        createdAt: date.toISOString(),
        parts: [
          {
            filename: compressedFilename,
            size: compressedStats.size,
            checksum,
          },
        ],
      };
      filesToUpload.push({ path: compressedFilepath, name: compressedFilename });
    }

    // Step 5: Save manifest
    const manifestPath = `${tempDir}/manifest.json`;
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2));

    // Step 6: Upload all parts with retry
    console.log("Uploading to Cloudinary...");

    for (let i = 0; i < filesToUpload.length; i++) {
      const { path, name } = filesToUpload[i];
      console.log(`Uploading ${i + 1}/${filesToUpload.length}: ${name}...`);

      try {
        await uploadToCloudinaryWithRetry({
          name,
          path,
          folder: backupFolder,
        });
        uploadedFiles.push(name);
      } catch (error) {
        // Rollback: delete already uploaded files
        await deleteCloudinaryFiles(backupFolder, uploadedFiles);
        throw error;
      }
    }

    // Step 7: Upload manifest
    console.log("Uploading manifest...");
    try {
      await uploadToCloudinaryWithRetry({
        name: "manifest.json",
        path: manifestPath,
        folder: backupFolder,
      });
      uploadedFiles.push("manifest.json");
    } catch (error) {
      await deleteCloudinaryFiles(backupFolder, uploadedFiles);
      throw error;
    }

    console.log(`\nBackup uploaded successfully to: ${backupFolder}`);
    console.log(`Total parts: ${manifest.totalParts}`);
    console.log(`Total size: ${(manifest.totalSize / 1024 / 1024).toFixed(2)}MB`);

  } catch (error) {
    console.log("An error occurred!", error);
    throw error;
  } finally {
    // Always cleanup temp directory
    await cleanupTempDir(tempDir);
  }

  console.log("DB backup complete...");
};
