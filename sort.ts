import { createReadStream, createWriteStream } from "fs";
import { rm } from "fs/promises";
import { pipeline } from "stream/promises";
import readline from "readline";

const BUFFER_CAPACITY = 100_000_00;
const MAX_MEM_USE = 100_000_00;
const FILE_SIZE = 200_000_00;

(async function () {
  const fileName = "logistic-chaos.txt";

  await createLargeFile(fileName);
  await externSort(fileName);
})();

async function externSort(fileName: string) {
  const file = createReadStream(fileName, { highWaterMark: BUFFER_CAPACITY });
  const lines = readline.createInterface({ input: file, crlfDelay: Infinity });
  const v: number[] = [];
  let size = 0;
  const tmpFileNames: string[] = [];
  for await (let line of lines) {
    size += line.length;
    v.push(parseFloat(line));
    if (size > MAX_MEM_USE) {
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

function cleanUp(tmpFileNames: string[]) {
  return Promise.all(tmpFileNames.map((f) => rm(f)));
}

async function merge(tmpFileNames: string[], fileName: string) {
  console.log("merging result ...");
  const resultFileName = `${fileName.split(".txt")[0]}-sorted.txt`;
  const file = createWriteStream(resultFileName, {
    highWaterMark: BUFFER_CAPACITY,
  });
  const activeReaders = tmpFileNames.map((name) =>
    readline
      .createInterface({
        input: createReadStream(name, { highWaterMark: BUFFER_CAPACITY }),
        crlfDelay: Infinity,
      })
      [Symbol.asyncIterator]()
  );
  const values = await Promise.all<number>(
    activeReaders.map((r) => {
      return r.next().then((e) => {
        return parseFloat(e.value);
      });
    })
  );

  return pipeline(async function* () {
    while (activeReaders.length > 0) {
      const [minVal, i] = values.reduce(
        (prev, cur, idx) => (cur < prev[0] ? [cur, idx] : prev),
        [Infinity, -1]
      );
      yield `${minVal}\n`;
      const res = await activeReaders[i].next();
      if (!res.done) {
        values[i] = parseFloat(res.value);
      } else {
        values.splice(i, 1);
        activeReaders.splice(i, 1);
      }
    }
    console.log("Done merging");
  }, file);
}

async function sortAndWriteToFile(v: number[], tmpFileNames: string[]) {
  v.sort((a, b) => a - b);
  let tmpFileName = `tmp_sort_${tmpFileNames.length}.txt`;
  tmpFileNames.push(tmpFileName);
  console.log(`creating tmp file: ${tmpFileName}`);
  await pipeline(
    v.map((e) => `${e}\n`),
    createWriteStream(tmpFileName, { highWaterMark: BUFFER_CAPACITY })
  );
  v.length = 0;
}

function createLargeFile(fileName: string) {
  console.log("Creating large file ...");
  return pipeline(
    logistic(0.35),
    createWriteStream(fileName, { highWaterMark: BUFFER_CAPACITY })
  );
}

function* logistic(x: number): Generator<string> {
  let readBytes = 0;
  let lastLog = 0;
  while (readBytes < FILE_SIZE) {
    x = 3.7 * x * (1.0 - x);
    const data = `${x}\n`;
    readBytes += data.length;
    if (readBytes - lastLog > 1_000_000) {
      console.log(`${readBytes / 1_000_000.0}mb`);
      lastLog = readBytes;
    }
    yield data;
  }
}
