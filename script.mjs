import https from "https";
import http from "http";
import fs from "fs/promises";
import { parse } from "node-html-parser";
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

// Extract form fields from HTML
function extractFormFields(html) {
  const root = parse(html);
  return {
    viewstate: root.querySelector("#__VIEWSTATE")?.getAttribute("value") || "",
    viewstateGenerator:
      root.querySelector("#__VIEWSTATEGENERATOR")?.getAttribute("value") || "",
    eventValidation:
      root.querySelector("#__EVENTVALIDATION")?.getAttribute("value") || "",
  };
}

// Extract cookies from response headers
function extractCookies(response) {
  return (
    response.headers["set-cookie"]
      ?.map((cookie) => cookie.split(";")[0])
      .join("; ") || ""
  );
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

  console.log("Getting BSE cookies and viewstate...");
  const initialOptions = {
    hostname: "www.bseindia.com",
    path: "/markets/debt/debt_search.aspx",
    method: "GET",
    headers: {
      "user-agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
      accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
      "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    },
  };

  try {
    // Step 1: GET the page to obtain viewstate and cookies
    const initialResponse = await makeRequest(initialOptions);

    if (initialResponse.statusCode !== 200) {
      throw new Error(`GET returned status ${initialResponse.statusCode}`);
    }

    const cookies = extractCookies(initialResponse);
    const fields1 = extractFormFields(initialResponse.body);

    if (
      !fields1.viewstate ||
      !fields1.viewstateGenerator ||
      !fields1.eventValidation
    ) {
      throw new Error("Failed to extract viewstate parameters");
    }

    console.log("✓ Got viewstate parameters");

    // Step 2: Submit search to load results (needed for download viewstate)
    console.log("Submitting BSE form...");
    const dateStr = formatDateBSE(targetDate);

    const submitData = new URLSearchParams({
      __VIEWSTATE: fields1.viewstate,
      __VIEWSTATEGENERATOR: fields1.viewstateGenerator,
      __VIEWSTATEENCRYPTED: "",
      __EVENTVALIDATION: fields1.eventValidation,
      ctl00$ContentPlaceHolder1$hidFDate: "",
      ctl00$ContentPlaceHolder1$txtFromDate: dateStr,
      ctl00$ContentPlaceHolder1$txtTodate: dateStr,
      ctl00$ContentPlaceHolder1$btnSubmit: "Submit",
      ctl00$ContentPlaceHolder1$hidCurrentDate: "",
      ctl00$ContentPlaceHolder1$Indices: "rcds",
    }).toString();

    const submitOptions = {
      hostname: "www.bseindia.com",
      path: "/markets/debt/debt_search.aspx",
      method: "POST",
      headers: {
        accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded",
        "content-length": Buffer.byteLength(submitData),
        cookie: cookies,
        origin: "https://www.bseindia.com",
        referer: "https://www.bseindia.com/markets/debt/debt_search.aspx",
        "user-agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
      },
    };

    const submitResponse = await makeRequest(submitOptions, submitData);

    if (submitResponse.statusCode !== 200) {
      throw new Error(
        `BSE submit POST returned status code: ${submitResponse.statusCode}`,
      );
    }

    console.log("✓ Got BSE search response");

    // Check if the search result page has any data
    if (!submitResponse.body.includes("ContentPlaceHolder1_divCT1")) {
      throw new Error("No bond data found in BSE response");
    }

    // Step 3: Trigger CSV download using the post-search viewstate
    const fields2 = extractFormFields(submitResponse.body);
    if (
      !fields2.viewstate ||
      !fields2.viewstateGenerator ||
      !fields2.eventValidation
    ) {
      throw new Error("Failed to extract viewstate from search response");
    }

    console.log("Downloading BSE CSV...");

    const downloadData = new URLSearchParams({
      __EVENTTARGET: "ctl00$ContentPlaceHolder1$imgDownload",
      __EVENTARGUMENT: "",
      __VIEWSTATE: fields2.viewstate,
      __VIEWSTATEGENERATOR: fields2.viewstateGenerator,
      __VIEWSTATEENCRYPTED: "",
      __EVENTVALIDATION: fields2.eventValidation,
      ctl00$ContentPlaceHolder1$hidFDate: "",
      ctl00$ContentPlaceHolder1$txtFromDate: dateStr,
      ctl00$ContentPlaceHolder1$txtTodate: dateStr,
      ctl00$ContentPlaceHolder1$hidCurrentDate: "",
      ctl00$ContentPlaceHolder1$Indices: "rcds",
    }).toString();

    const downloadOptions = {
      hostname: "www.bseindia.com",
      path: "/markets/debt/debt_search.aspx",
      method: "POST",
      headers: {
        accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded",
        "content-length": Buffer.byteLength(downloadData),
        cookie: cookies,
        origin: "https://www.bseindia.com",
        referer: "https://www.bseindia.com/markets/debt/debt_search.aspx",
        "user-agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
      },
    };

    const downloadResponse = await makeRequest(downloadOptions, downloadData);

    if (downloadResponse.statusCode !== 200) {
      throw new Error(
        `BSE download POST returned status code: ${downloadResponse.statusCode}`,
      );
    }

    // If BSE returned HTML instead of CSV, there's no downloadable data
    const body = downloadResponse.body.trim();
    if (
      body.startsWith("<!") ||
      body.startsWith("<html") ||
      body.startsWith("<")
    ) {
      throw new Error(
        "No bond data found in BSE response (HTML returned instead of CSV)",
      );
    }

    console.log("✓ BSE CSV downloaded successfully");
    return downloadResponse.body;
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
  console.log(csvData.slice(0, 500));
  // Parse CSV directly (like migrate-bse.mjs) to avoid issues with parseCSV wrapper
  let csvStr = csvData;
  if (csvStr.charCodeAt(0) === 0xfeff) {
    csvStr = csvStr.slice(1);
  }

  const records = parseCSVLib(csvStr, {
    columns: (headers) => headers.map(normalizeHeader),
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: true,
    bom: true,
  });

  if (!records.length) {
    throw new Error("No BSE records to store");
  }

  // Log CSV columns for debugging (first time only)
  if (records.length > 0 && !storeBSEData._headerLogged) {
    console.log(`  BSE CSV columns: ${Object.keys(records[0]).join(", ")}`);
    storeBSEData._headerLogged = true;
  }

  const dbRecords = records.map((r) => {
    // Handle different CSV column name variations
    // Try "Scrip Name" first (from migrate-bse.mjs format), fallback to "Security Code"
    const securityCode =
      r["Scrip Name"] || r["Security Code"] || r["Scrip Code"] || null;

    // Try "Close Price" first, fallback to "LTP"
    const closePrice = r["Close Price"] || r["LTP"] || null;
    const ltp = closePrice
      ? parseFloat(String(closePrice).replace(/,/g, ""))
      : null;

    // Try "Total Trade Turnover (Rs. Lakhs)" first, fallback to "Turnover in Lakhs"
    const turnoverField =
      r["Total Trade Turnover (Rs. Lakhs)"] ||
      r["Turnover in Lakhs"] ||
      r["Turnover (Rs Lacs)"] ||
      null;
    const turnoverRsLacs = turnoverField
      ? parseFloat(String(turnoverField).replace(/,/g, ""))
      : null;

    // Try "Total Trade Volume" first, fallback to "No.Of Trades" or "No Of Trades"
    const volumeField =
      r["Total Trade Volume"] ||
      r["No.Of Trades"] ||
      r["No Of Trades"] ||
      r["No Of Trades"] ||
      null;
    const noOfTrades = volumeField
      ? parseInt(String(volumeField).replace(/,/g, ""), 10)
      : null;

    return {
      trade_date: tradeDate,
      exchange: "BSE",
      security_code: securityCode,
      issuer_name: null,
      coupon_rate: null,
      maturity_date: null,
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
