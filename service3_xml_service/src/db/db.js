const { Pool } = require("pg");

let pool;

if (process.env.DATABASE_URL) {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });
}
else {
  pool = new Pool({
    host: process.env.XML_DB_HOST,
    port: process.env.XML_DB_PORT,
    database: process.env.XML_DB_NAME,
    user: process.env.XML_DB_USER,
    password: process.env.XML_DB_PASSWORD,
  });
}

async function insertXmlDocument(xmlContent, mapperVersion) {
  const query = `
    INSERT INTO xml_documents (xml_documento, mapper_version)
    VALUES ($1::xml, $2)
    RETURNING id
  `;

  const result = await pool.query(query, [xmlContent, mapperVersion]);
  return result.rows[0].id;
}

module.exports = {
  insertXmlDocument,
};
