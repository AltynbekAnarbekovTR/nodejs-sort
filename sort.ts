import { createReadStream, createWriteStream } from "fs";
import { rm } from "fs/promises";
import { pipeline } from "stream/promises";
import readline from "readline";

// Values are in bytes. 1000000 = 1 mb
const bufferCapacity = 1000000;
const maxMemoryAvailable = 1000000;
const fileSize = 20000000;

(async function () {
  const fileName = "largeFile.txt";

  await createLargeFile(fileName);
  await externSort(fileName);
})();

function* generateRandomString(): Generator {
  let readBytes = 0;
  let lastLog = 0;
  while (readBytes < fileSize) {
    const length = 12;
    let result = "";
    const characters =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    const x = result;
    const data = `${x}\n`;
    readBytes += data.length;
    if (readBytes - lastLog > 1_000_000) {
      console.log(`${readBytes / 1_000_000.0}mb`);
      lastLog = readBytes;
    }
    yield data;
  }
}

function createLargeFile(fileName: string) {
  console.log("Creating large file ...");
  return pipeline(
    generateRandomString(),
    createWriteStream(fileName, { highWaterMark: bufferCapacity })
  );
}

async function externSort(fileName: string) {
  const file = createReadStream(fileName, { highWaterMark: bufferCapacity });
  const lines = readline.createInterface({ input: file, crlfDelay: Infinity });
  const v: string[] = [];
  let size = 0;
  const tmpFileNames: string[] = [];
  for await (let line of lines) {
    size += line.length;

    v.push(line);
    if (size > maxMemoryAvailable) {
      await sortAndWriteToFile(v, tmpFileNames);
      size = 0;
    }
  }
  if (v.length > 0) {
    await sortAndWriteToFile(v, tmpFileNames);
  }
  await merge(tmpFileNames, fileName);
  await cleanUp(tmpFileNames);
}

async function sortAndWriteToFile(v: string[], tmpFileNames: string[]) {
  v.sort();
  let tmpFileName = `tmp_sort_${tmpFileNames.length}.txt`;
  tmpFileNames.push(tmpFileName);
  console.log(`creating tmp file: ${tmpFileName}`);
  await pipeline(
    v.map((e) => `${e}\n`),
    createWriteStream(tmpFileName, { highWaterMark: bufferCapacity })
  );
  v.length = 0;
}

function cleanUp(tmpFileNames: string[]) {
  return Promise.all(tmpFileNames.map((f) => rm(f)));
}

async function merge(tmpFileNames: string[], fileName: string) {
  console.log("merging result ...");
  const resultFileName = `${fileName.split(".txt")[0]}-sorted.txt`;
  const file = createWriteStream(resultFileName, {
    highWaterMark: bufferCapacity,
  });
  const activeReaders = tmpFileNames.map((name) =>
    readline
      .createInterface({
        input: createReadStream(name, { highWaterMark: bufferCapacity }),
        crlfDelay: Infinity,
      })
      [Symbol.asyncIterator]()
  );
  const values = await Promise.all<string>(
    activeReaders.map((r) => {
      return r.next().then((e) => {
        return e.value;
      });
    })
  );

  return pipeline(async function* () {
    while (activeReaders.length > 0) {
      const [minVal, i] = values.reduce(
        (prev, cur, idx) => (cur < prev[0] ? [cur, idx] : prev),
        ["zzzzzzzzzz", -1]
      );
      yield `${minVal}\n`;
      const res = await activeReaders[i].next();
      if (!res.done) {
        values[i] = res.value;
      } else {
        values.splice(i, 1);
        activeReaders.splice(i, 1);
      }
    }
  }, file);
}
