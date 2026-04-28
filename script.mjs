import https from "https";
import http from "http";
import fs from "fs/promises";
import { createClient } from "@supabase/supabase-js";
import { tmpdir } from "os";
import { join } from "path";
import dotenv from "dotenv";
import TelegramBot from "node-telegram-bot-api";
import { parse as parseCSVLib } from "csv-parse/sync";

dotenv.config();

// Initialize Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

// Initialize Telegram bot
const bot = new TelegramBot(process.env.TELEGRAM_API_TOKEN, { polling: false });

// Send Telegram notification
async function sendTelegramMessage(text) {
  try {
    await bot.sendMessage(process.env.TELEGRAM_CHANNEL, text, {
      parse_mode: "HTML",
    });
    console.log("Sent message on telegram");
  } catch (err) {
    console.error("Failed to send Telegram message:", err);
  }
}

// Save error to Supabase storage
async function saveErrorToStorage(errorDetails) {
  try {
    const fileName = `bond-scraper-error-${new Date().toISOString()}.json`;
    const filePath = join(tmpdir(), fileName);
    await fs.writeFile(filePath, JSON.stringify(errorDetails, null, 2));

    const file = await fs.readFile(filePath);
    const { error } = await supabase.storage
      .from("data-dump")
      .upload(`bond-scraper-errors/${fileName}`, file, {
        contentType: "application/json",
        upsert: true,
      });

    // Clean up temp file
    await fs.unlink(filePath).catch(() => {});

    if (error) {
      console.error("Failed to save error to storage:", error);
      return false;
    }

    console.log("Error details saved to storage");
    return true;
  } catch (err) {
    console.error("Exception while saving error to storage:", err);
    return false;
  }
}

// Utility function to make HTTPS requests
function makeRequest(options, postData = null) {
  return new Promise((resolve, reject) => {
    const protocol = options.protocol === "http:" ? http : https;
    const req = protocol.request(options, (res) => {
      let data = "";

      res.on("data", (chunk) => {
        data += chunk;
      });

      res.on("end", () => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: data,
        });
      });
    });

    req.on("error", (error) => {
      reject(error);
    });

    if (postData) {
      req.write(postData);
    }

    req.end();
  });
}

function parseBSEDate(d) {
  if (!d) return null;
  const [dd, mm, yyyy] = d.split("/");
  return `${yyyy}-${mm}-${dd}`;
}

// Get the appropriate date based on current time
// Returns the last working day (Mon-Fri)
function getTargetDate() {
  const now = new Date();
  const istOffset = 5.5 * 60 * 60 * 1000;
  const istTime = new Date(now.getTime() + istOffset);
  const hour = istTime.getUTCHours();
  const dayOfWeek = istTime.getUTCDay(); // 0=Sun, 1=Mon, ..., 6=Sat

  let targetDate = new Date(istTime);

  // If before 3 PM IST, use previous day's data
  if (hour < 15) {
    targetDate.setDate(targetDate.getDate() - 1);
  }

  // Now ensure we have a working day (Mon-Fri)
  const targetDay = targetDate.getUTCDay();

  if (targetDay === 0) {
    // Sunday -> go back to Friday
    targetDate.setDate(targetDate.getDate() - 2);
  } else if (targetDay === 6) {
    // Saturday -> go back to Friday
    targetDate.setDate(targetDate.getDate() - 1);
  }

  return targetDate;
}

// Format date as DD/MM/YYYY for BSE
function formatDateBSE(date) {
  const day = String(date.getUTCDate()).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const year = date.getUTCFullYear();
  return `${day}/${month}/${year}`;
}

function formatDateBSEApi(date) {
  const day = String(date.getUTCDate()).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const year = date.getUTCFullYear();
  return `${year}${month}${day}`;
}

// Format date as YYYY-MM-DD for database
function formatDateDB(date) {
  const day = String(date.getUTCDate()).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const year = date.getUTCFullYear();
  return `${year}-${month}-${day}`;
}

function normalizeHeader(h) {
  return h.replace(/\n/g, " ").replace(/\s+/g, " ").trim();
}

function getRecordValue(record, keys) {
  for (const key of keys) {
    const value = record[key];
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return value;
    }
  }
  return null;
}

function parseOptionalNumber(value) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return null;
  }

  const parsed = parseFloat(String(value).replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function parseOptionalInteger(value) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return null;
  }

  const parsed = parseInt(String(value).replace(/,/g, ""), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

// Parse CSV string to array of objects
function parseCSV(csvString, exchange) {
  if (csvString.charCodeAt(0) === 0xfeff) {
    csvString = csvString.slice(1);
  }

  const records = parseCSVLib(csvString, {
    columns: (headers) => headers.map(normalizeHeader),
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: true,
    bom: true,
  });

  return records.map((r) => ({ ...r, exchange }));
}

function parseTabularText(rawText) {
  let text = rawText;
  if (text.charCodeAt(0) === 0xfeff) {
    text = text.slice(1);
  }

  const firstLine = text.split(/\r?\n/, 1)[0] || "";
  const delimiter = firstLine.includes("\t") ? "\t" : ",";

  return parseCSVLib(text, {
    columns: (headers) => headers.map(normalizeHeader),
    delimiter,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: true,
    bom: true,
  });
}

// Fetch NSE bond data
async function fetchNSEData(targetDate) {
  console.log("Fetching NSE bond data...");

  const options = {
    hostname: "www.nseindia.com",
    path: "/api/liveBonds-traded-on-cm?type=bonds&csv=true&selectValFormat=crores",
    method: "GET",
    headers: {
      accept: "*/*",
      "user-agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
      "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    },
  };

  try {
    const response = await makeRequest(options);

    if (response.statusCode === 200) {
      console.log("✓ NSE data fetched successfully");
      return response.body;
    } else {
      throw new Error(`NSE API returned status code: ${response.statusCode}`);
    }
  } catch (error) {
    console.error("Error fetching NSE data:", error.message);
    throw error;
  }
}

// Fetch BSE bond data
async function fetchBSEData(targetDate) {
  console.log("Fetching BSE bond data...");

  try {
    const apiDate = formatDateBSEApi(targetDate);
    const response = await makeRequest({
      hostname: "api.bseindia.com",
      path: `/BseIndiaAPI/api/rbcorpbonds1_download/w?frmDate=${apiDate}&toDate=${apiDate}&flag=1`,
      method: "GET",
      headers: {
        accept: "*/*",
        "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        dnt: "1",
        priority: "u=0, i",
        referer: "https://www.bseindia.com/",
        "user-agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
      },
    });

    if (response.statusCode !== 200) {
      throw new Error(`BSE API returned status code: ${response.statusCode}`);
    }

    const body = response.body.trim();
    if (!body) {
      throw new Error("BSE API returned an empty response");
    }

    if (
      body.startsWith("<!DOCTYPE") ||
      body.startsWith("<html") ||
      body.startsWith("<")
    ) {
      throw new Error("BSE download API returned HTML instead of tabular data");
    }

    const records = parseTabularText(body);
    if (!Array.isArray(records) || records.length === 0) {
      throw new Error("No bond data found in BSE download response");
    }

    console.log(`✓ BSE download API returned ${records.length} records`);
    return records;
  } catch (error) {
    console.error("Error fetching BSE data:", error.message);
    throw error;
  }
}

// Store NSE data in database
async function storeNSEData(csvData, tradeDate) {
  const records = parseCSV(csvData, "NSE");
  if (!records.length) throw new Error("No NSE records to store");
  const dbRecords = records.map((r) => {
    // VALUE (₹ Crores) → lakhs (1 crore = 100 lakhs)
    const turnoverLakhs = r["VALUE (₹ Crores)"]
      ? parseFloat(r["VALUE (₹ Crores)"].replace(/,/g, "")) * 100
      : null;

    return {
      trade_date: tradeDate,
      exchange: "NSE",

      security_code: r["SYMBOL"] || null,
      issuer_name: null, // Not available in NSE CSV
      coupon_rate: r["COUPON RATE"]
        ? parseFloat(r["COUPON RATE"].replace(/,/g, ""))
        : null,
      maturity_date:
        !r["MATURITY DATE"] || r["MATURITY DATE"] === "-"
          ? null
          : r["MATURITY DATE"],
      ltp: r["LTP"] ? parseFloat(r["LTP"].replace(/,/g, "")) : null,
      turnover_rs_lacs: turnoverLakhs,
      no_of_trades: r["VOLUME (Shares)"]
        ? parseInt(r["VOLUME (Shares)"].replace(/,/g, ""))
        : null,

      bond_type: r["BOND TYPE"] || null,
      face_value: r["FACE VALUE"]
        ? parseFloat(r["FACE VALUE"].replace(/,/g, ""))
        : null,
      credit_rating: r["CREDIT RATING"] || null,
      raw_data: r,
    };
  });

  await supabase
    .from("bond_trades")
    .delete()
    .eq("trade_date", tradeDate)
    .eq("exchange", "NSE");

  const { error } = await supabase.from("bond_trades").insert(dbRecords);
  if (error) throw new Error(error.message);

  return dbRecords.length;
}

// Store BSE data in database
async function storeBSEData(csvData, tradeDate) {
  const records = Array.isArray(csvData)
    ? csvData
    : (() => {
        console.log(csvData.slice(0, 500));
        return parseTabularText(csvData);
      })();

  if (!records.length) {
    throw new Error("No BSE records to store");
  }

  // Log CSV columns for debugging (first time only)
  if (records.length > 0 && !storeBSEData._headerLogged) {
    console.log(`  BSE CSV columns: ${Object.keys(records[0]).join(", ")}`);
    storeBSEData._headerLogged = true;
  }

  const dbRecords = records.map((r) => {
    const securityCode = getRecordValue(r, [
      "Security Code",
      "Scrip Name",
      "Scrip Code",
      "Security Name",
      "securityCode",
      "scripCode",
      "scripName",
      "security_name",
      "security_code",
      "Scripname",
    ]);

    const ltp = parseOptionalNumber(
      getRecordValue(r, [
        "Close Price",
        "LTP",
        "Price",
        "closePrice",
        "close_price",
        "ltp",
      ]),
    );

    const turnoverRsLacs = parseOptionalNumber(
      getRecordValue(r, [
        "Total Trade Turnover (Rs. Lakhs)",
        "Turnover in Lakhs",
        "Turnover (Rs Lacs)",
        "Turnover (Lacs)",
        "Trade Turnover",
        "tradeTurnover",
        "turnoverInLakhs",
        "turnover_rs_lacs",
      ]),
    );

    const noOfTrades = parseOptionalInteger(
      getRecordValue(r, [
        "Total Trade Volume",
        "No.Of Trades",
        "No Of Trades",
        "Total Trades",
        "tradeVolume",
        "totalTradeVolume",
        "noOfTrades",
        "no_of_trades",
      ]),
    );

    return {
      trade_date: tradeDate,
      exchange: "BSE",
      security_code: securityCode,
      issuer_name: getRecordValue(r, ["Issuer Name", "issuerName", "issuer_name"]),
      coupon_rate: parseOptionalNumber(
        getRecordValue(r, ["Coupon (%)", "Coupon", "coupon", "coupon_rate"]),
      ),
      maturity_date:
        getRecordValue(r, [
          "Maturity Date",
          "maturityDate",
          "maturity_date",
        ]) || null,
      ltp: ltp,
      turnover_rs_lacs: turnoverRsLacs,
      no_of_trades: noOfTrades,
      bond_type: null,
      face_value: null,
      credit_rating: null,
      raw_data: r,
    };
  });

  await supabase
    .from("bond_trades")
    .delete()
    .eq("trade_date", tradeDate)
    .eq("exchange", "BSE");

  const { error } = await supabase.from("bond_trades").insert(dbRecords);
  if (error) throw new Error(error.message);

  return dbRecords.length;
}

// Clean up old data (older than 90 days)
async function cleanupOldData() {
  console.log("Cleaning up data older than 90 days...");

  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - 90);
  const cutoffDateStr = formatDateDB(cutoffDate);

  const { data, error } = await supabase
    .from("bond_trades")
    .delete()
    .lt("trade_date", cutoffDateStr)
    .select();

  if (error) {
    console.error("Failed to cleanup old data:", error);
    return { count: 0, details: null };
  }

  const deletedCount = data?.length || 0;

  // Get breakdown by exchange and date range
  let details = null;
  if (deletedCount > 0 && data) {
    const byExchange = data.reduce((acc, row) => {
      acc[row.exchange] = (acc[row.exchange] || 0) + 1;
      return acc;
    }, {});

    const dates = data.map((row) => row.trade_date).sort();
    const oldestDate = dates[0];
    const newestDate = dates[dates.length - 1];

    details = {
      byExchange,
      dateRange: { oldest: oldestDate, newest: newestDate },
      cutoffDate: cutoffDateStr,
    };
  }

  console.log(`✓ Cleaned up ${deletedCount} old records`);
  return { count: deletedCount, details };
}

// Main execution
async function main() {
  console.log("=== Bond Data Scraper ===");
  console.log(`Started at: ${new Date().toISOString()}`);

  const targetDate = getTargetDate();
  const tradeDateStr = formatDateDB(targetDate);
  const displayDate = formatDateBSE(targetDate);

  console.log(`Target date: ${displayDate}`);
  console.log("");

  const results = {
    nse: { success: false, count: 0, error: null },
    bse: { success: false, count: 0, error: null },
    cleanup: { count: 0, details: null },
  };

  // Fetch and store NSE data
  try {
    const nseData = await fetchNSEData(targetDate);
    const count = await storeNSEData(nseData, tradeDateStr);
    results.nse.success = true;
    results.nse.count = count;
  } catch (error) {
    results.nse.error = error.message;
    console.error("NSE processing failed:", error.message);
  }

  console.log("");

  // Fetch and store BSE data
  try {
    const bseData = await fetchBSEData(targetDate);
    const count = await storeBSEData(bseData, tradeDateStr);
    results.bse.success = true;
    results.bse.count = count;
  } catch (error) {
    results.bse.error = error.message;
    console.error("BSE processing failed:", error.message);
  }

  console.log("");

  // Cleanup old data
  try {
    const cleanupResult = await cleanupOldData();
    results.cleanup.count = cleanupResult.count;
    results.cleanup.details = cleanupResult.details;
  } catch (error) {
    console.error("Cleanup failed:", error.message);
  }

  console.log("");
  console.log("=== Summary ===");

  // Prepare Telegram message
  let telegramMsg = `📊 <b>Bond Data Scraper - ${displayDate}</b>\n\n`;

  if (results.nse.success) {
    console.log(`✓ NSE: Stored ${results.nse.count} records`);
    telegramMsg += `✅ <b>NSE:</b> Stored ${results.nse.count} records\n`;
  } else {
    console.log(`❌ NSE: Failed - ${results.nse.error}`);
    telegramMsg += `❌ <b>NSE:</b> Failed\n`;
  }

  if (results.bse.success) {
    console.log(`✓ BSE: Stored ${results.bse.count} records`);
    telegramMsg += `✅ <b>BSE:</b> Stored ${results.bse.count} records\n`;
  } else {
    console.log(`❌ BSE: Failed - ${results.bse.error}`);
    telegramMsg += `❌ <b>BSE:</b> Failed\n`;
  }

  if (results.cleanup.count > 0) {
    const { count, details } = results.cleanup;
    console.log(`✓ Cleanup: Removed ${count} old records`);

    telegramMsg += `\n🧹 <b>Cleanup: Deleted ${count} old records</b>\n`;
    if (details) {
      telegramMsg += `📅 Date range: ${details.dateRange.oldest} to ${details.dateRange.newest}\n`;
      telegramMsg += `🗓️ Cutoff: ${details.cutoffDate}\n`;

      if (details.byExchange.NSE) {
        telegramMsg += `  • NSE: ${details.byExchange.NSE} records\n`;
      }
      if (details.byExchange.BSE) {
        telegramMsg += `  • BSE: ${details.byExchange.BSE} records\n`;
      }
    }
  }

  // Handle errors
  if (results.nse.error || results.bse.error) {
    const errorDetails = {
      timestamp: new Date().toISOString(),
      date: displayDate,
      errors: {
        nse: results.nse.error,
        bse: results.bse.error,
      },
    };

    const saved = await saveErrorToStorage(errorDetails);
    if (!saved) {
      telegramMsg += `\n⚠️ Could not save error details to storage\n`;
    }

    telegramMsg += `\n<i>Errors:</i>\n`;
    if (results.nse.error) telegramMsg += `• NSE: ${results.nse.error}\n`;
    if (results.bse.error) telegramMsg += `• BSE: ${results.bse.error}\n`;
  }

  telegramMsg += `\n⏰ Completed: ${new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" })}`;

  // Send Telegram notification
  await sendTelegramMessage(telegramMsg);

  console.log(`\nCompleted at: ${new Date().toISOString()}`);

  // Exit with error code if both failed
  if (!results.nse.success && !results.bse.success) {
    process.exit(1);
  }
}

// Run the script
main().catch(async (error) => {
  console.error("Fatal error:", error);
  await sendTelegramMessage(
    `🚨 <b>Bond Scraper Failed</b>\n\n${error.message}`,
  );
  process.exit(1);
});
