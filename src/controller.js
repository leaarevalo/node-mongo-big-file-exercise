const Records = require("./records.model");
const { parse } = require("fast-csv");
const fs = require("fs");

const batchSize = 1000; 
const upload = async (req, res) => {
  const { file } = req;
  let data = [];
  let processing = Promise.resolve();
  let insertedCount = 0;
  const stream = fs.createReadStream(file.path);

  stream
    .pipe(parse({ headers: true, skipEmptyLines: true }))
    .on("data", (row) => {
      data.push(row);
      if (data.length >= batchSize) {
        const batch = [...data];
        data = [];
        stream.pause();

        processing = processing
          .then(() => {
            return Records.insertMany(batch, { ordered: false });
          })
          .then((result) => {
            insertedCount += result.length;
            stream.resume();
          })
          .catch((err) => {
            stream.destroy();
            throw err;
          });
      }
    })
    .on("end", async () => {
      await processing;
      if (data.length > 0) {
            const result =  await Records.insertMany(data, { ordered: false });
            insertedCount += result.length;
      }
      fs.unlink(file.path, (err) => {
        if (err) {
          console.error("Error deleting file:", err);
        }
      });
      return res.status(200).json({
        message: "File processed successfully",
        inserted: insertedCount,
      });
    })
    .on("error", (err) => {
      return res.status(500).json({ error: "Error processing file" });
    });
};

const list = async (_, res) => {
  try {
    const data = await Records.find({}).limit(10).lean();

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json(err);
  }
};

module.exports = {
  upload,
  list,
};
