const fs = require("fs");
const path = require("path");
const libxml = require("libxmljs2");

function validateWellFormedXml(xmlPath) {
  if (!fs.existsSync(xmlPath)) {
    throw new Error("XML file does not exist");
  }

  const stats = fs.statSync(xmlPath);
  if (stats.size === 0) {
    throw new Error("XML file is empty");
  }

  const xmlContent = fs.readFileSync(xmlPath, "utf-8");

  try {
    return libxml.parseXml(xmlContent);
  } catch (err) {
    throw new Error(`XML not well-formed: ${err.message}`);
  }
}

function validateXmlWithXsd(xmlPath, xsdPath) {
  const xmlContent = fs.readFileSync(xmlPath, "utf-8");
  const xsdContent = fs.readFileSync(xsdPath, "utf-8");

  let xmlDoc;
  let xsdDoc;

  try {
    xmlDoc = libxml.parseXml(xmlContent);
  } catch (e) {
    throw new Error("Failed to parse XML document");
  }

  try {
    xsdDoc = libxml.parseXml(xsdContent);
  } catch (e) {
    throw new Error("Failed to parse XSD schema");
  }

  const isValid = xmlDoc.validate(xsdDoc);

  if (!isValid) {
    const errors = xmlDoc.validationErrors
      .map(err => err.message)
      .join("; ");
    throw new Error(`XSD validation failed: ${errors}`);
  }

  return true;
}


module.exports = {
  validateWellFormedXml,
  validateXmlWithXsd,
};
