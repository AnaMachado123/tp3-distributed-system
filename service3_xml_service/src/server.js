require("dotenv").config();

const express = require("express");
const multer = require("multer");
const path = require("path");

const { writeXmlFromCsv } = require("./xml/xml_writer");
const {
  validateWellFormedXml,
  validateXmlWithXsd,
} = require("./xml/xml_validator");
const { insertXmlDocument } = require("./db/db");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

/**
 * Health check
 */
app.get("/health", (req, res) => {
  res.status(200).json({ service: "xml-service", status: "ok" });
});

/**
 * Gate principal: recebe CSV + metadata do Processador de Dados
 */
app.post("/xml/import", upload.single("csv_file"), async (req, res) => {
  const { request_id, mapper_version, webhook_url } = req.body;
  const file = req.file;

  // 1️⃣ validação mínima do contrato
  if (!request_id || !mapper_version || !webhook_url || !file) {
    return res.status(400).json({
      error: "missing_fields",
      required: ["request_id", "mapper_version", "webhook_url", "csv_file"],
    });
  }

  try {
    // 2️⃣ criar XML a partir do CSV (streaming → ficheiro + memória)
    const { xmlPath, xmlContent } = await writeXmlFromCsv({
      csvBuffer: file.buffer,
      requestId: request_id,
      mapperVersion: mapper_version,
    });

    console.log("[XML SERVICE] XML criado em:", xmlPath);

    // 3️⃣ validar se é bem-formado
    validateWellFormedXml(xmlPath);
    console.log("[XML SERVICE] XML bem-formado confirmado");

    // 4️⃣ validar contra XSD
    const xsdPath = path.join(__dirname, "xml", "schema_v1.xsd");
    validateXmlWithXsd(xmlPath, xsdPath);
    console.log("[XML SERVICE] XML validado com XSD");

    // 5️⃣ persistir XML completo na BD (Supabase)
    const xmlDocumentId = await insertXmlDocument(
      xmlContent,
      mapper_version
    );

    console.log(
      `[XML SERVICE] XML persistido na BD (id=${xmlDocumentId})`
    );

    // 6️⃣ callback assíncrono (Webhook REST → Processador de Dados)
    try {
      await fetch(`${webhook_url}/webhook/xml-status`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-WEBHOOK-TOKEN": process.env.PD_WEBHOOK_TOKEN,
        },
        body: JSON.stringify({
          request_id,
          status: "OK",
          xml_document_id: xmlDocumentId,
        }),
      });

      console.log("[XML SERVICE] Webhook REST enviado para o PD");
    } catch (webhookErr) {
      console.error(
        "[XML SERVICE] Falha ao enviar webhook:",
        webhookErr.message
      );
      // ⚠️ erro de webhook NÃO invalida o processamento
    }

    // 7️⃣ resposta final imediata ao PD
    return res.status(202).json({
      status: "xml_persisted",
      request_id,
      xml_document_id: xmlDocumentId,
    });

  } catch (err) {
    console.error("[XML SERVICE] erro:", err.message);

    return res.status(500).json({
      status: "error",
      message: err.message,
    });
  }
});

const PORT = process.env.PORT || 9000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[XML SERVICE] listening on ${PORT}`);
});
