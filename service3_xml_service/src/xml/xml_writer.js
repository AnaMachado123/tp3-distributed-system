const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse");

async function writeXmlFromCsv({ csvBuffer, requestId, mapperVersion }) {
  const tmpDir = path.join(__dirname, "..", "..", "tmp");
  fs.mkdirSync(tmpDir, { recursive: true });

  const xmlPath = path.join(tmpDir, `${requestId}.xml`);
  const writeStream = fs.createWriteStream(xmlPath, { encoding: "utf-8" });

  // vamos acumular o XML em memória SEM remover o ficheiro
  let xmlContent = "";

  function write(line) {
    xmlContent += line;
    writeStream.write(line);
  }

  // Header XML
  write(`<?xml version="1.0" encoding="UTF-8"?>\n`);
  write(`<Request id="${requestId}" mapper="${mapperVersion}">\n`);
  write(`  <Countries>\n`);

  const parser = parse({
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });

  return new Promise((resolve, reject) => {
    parser.on("readable", () => {
      let record;
      while ((record = parser.read()) !== null) {
        const name = record.pais;
        const region = record.region;
        const cost = record.custo_vida_index;
        const population = record.population;

        write(
          `    <Country>\n` +
          `      <Name>${escapeXml(name)}</Name>\n` +
          `      <Region>${escapeXml(region)}</Region>\n` +
          `      <CostOfLiving index="${escapeXml(cost)}"/>\n` +
          `      <Population>${escapeXml(population)}</Population>\n` +
          `    </Country>\n`
        );
      }
    });

    parser.on("end", () => {
      write(`  </Countries>\n`);
      write(`</Request>\n`);
      writeStream.end();
    });

    writeStream.on("finish", () => {
      resolve({ xmlPath, xmlContent });
    });

    parser.on("error", (err) => {
      writeStream.destroy();
      reject(err);
    });

    // iniciar parsing
    parser.write(csvBuffer);
    parser.end();
  });
}

// Escapar XML (importante para dados reais)
function escapeXml(value) {
  if (value === undefined || value === null) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

module.exports = {
  writeXmlFromCsv,
};
