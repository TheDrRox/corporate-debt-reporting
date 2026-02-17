import https from "https";
import { parse } from "node-html-parser";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import TelegramBot from "node-telegram-bot-api";
import { parse as parseCSVLib } from "csv-parse/sync";

dotenv.config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const bot = new TelegramBot(process.env.TELEGRAM_API_TOKEN, { polling: false });

const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeRequest(options, postData = null) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
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
    req.on("error", reject);
    if (postData) req.write(postData);
    req.end();
  });
}

function formatDateBSE(date) {
  const day = String(date.getUTCDate()).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const year = date.getUTCFullYear();
  return `${day}/${month}/${year}`;
}

function formatDateDB(date) {
  const day = String(date.getUTCDate()).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const year = date.getUTCFullYear();
  return `${year}-${month}-${day}`;
}

function normalizeHeader(h) {
  return h.replace(/\n/g, " ").replace(/\s+/g, " ").trim();
}

function extractFormFields(html) {
  const root = parse(html, { insecureHTTPParser: true });
  return {
    viewstate: root.querySelector("#__VIEWSTATE")?.getAttribute("value") || "",
    viewstateGenerator:
      root.querySelector("#__VIEWSTATEGENERATOR")?.getAttribute("value") || "",
    eventValidation:
      root.querySelector("#__EVENTVALIDATION")?.getAttribute("value") || "",
  };
}

function extractCookies(response) {
  return (
    response.headers["set-cookie"]?.map((c) => c.split(";")[0]).join("; ") || ""
  );
}

const COMMON_HEADERS = {
  "user-agent": UA,
  accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
  "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
  "sec-ch-ua":
    '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"macOS"',
  "sec-fetch-dest": "document",
  "sec-fetch-mode": "navigate",
  "sec-fetch-site": "same-origin",
  "sec-fetch-user": "?1",
  "upgrade-insecure-requests": "1",
  dnt: "1",
};

async function fetchBSECSV(date) {
  const dateStr = formatDateBSE(date);

  // Step 1: GET the page to obtain viewstate and cookies
  const getResp = await makeRequest({
    hostname: "www.bseindia.com",
    path: "/markets/debt/debt_search.aspx",
    method: "GET",
    headers: { ...COMMON_HEADERS },
  });

  if (getResp.statusCode !== 200) {
    console.error(`GET returned status ${getResp.statusCode}`);
    return null;
  }

  const cookies = extractCookies(getResp);
  const fields1 = extractFormFields(getResp.body);
  if (!fields1.viewstate) {
    console.error("No viewstate from initial GET");
    return null;
  }

  // Step 2: Submit search to load results (needed for download viewstate)
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

  const submitResp = await makeRequest(
    {
      hostname: "www.bseindia.com",
      path: "/markets/debt/debt_search.aspx",
      method: "POST",
      headers: {
        ...COMMON_HEADERS,
        "content-type": "application/x-www-form-urlencoded",
        "content-length": Buffer.byteLength(submitData),
        cookie: cookies,
        origin: "https://www.bseindia.com",
        referer: "https://www.bseindia.com/markets/debt/debt_search.aspx",
        "cache-control": "max-age=0",
      },
    },
    submitData,
  );

  if (submitResp.statusCode !== 200) {
    console.error(`Submit POST returned status ${submitResp.statusCode}`);
    return null;
  }

  // Check if the search result page has any data
  if (!submitResp.body.includes("ContentPlaceHolder1_divCT1")) {
    return null;
  }

  const fields2 = extractFormFields(submitResp.body);
  if (!fields2.viewstate) {
    console.error("No viewstate from submit response");
    return null;
  }

  // Step 3: Trigger CSV download using the post-search viewstate
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

  const downloadResp = await makeRequest(
    {
      hostname: "www.bseindia.com",
      path: "/markets/debt/debt_search.aspx",
      method: "POST",
      headers: {
        ...COMMON_HEADERS,
        "content-type": "application/x-www-form-urlencoded",
        "content-length": Buffer.byteLength(downloadData),
        cookie: cookies,
        origin: "https://www.bseindia.com",
        referer: "https://www.bseindia.com/markets/debt/debt_search.aspx",
        "cache-control": "max-age=0",
      },
    },
    downloadData,
  );

  if (downloadResp.statusCode !== 200) {
    console.error(`Download POST returned status ${downloadResp.statusCode}`);
    return null;
  }

  // If BSE returned HTML instead of CSV, there's no downloadable data
  const body = downloadResp.body.trim();
  if (
    body.startsWith("<!") ||
    body.startsWith("<html") ||
    body.startsWith("<")
  ) {
    console.error("BSE returned HTML instead of CSV");
    return null;
  }

  return downloadResp.body;
}

async function main() {
  console.log("=== BSE Data Migration (Jan 1 - Feb 16, 2026) ===");
  console.log(`Started at: ${new Date().toISOString()}\n`);

  const startDate = new Date(Date.UTC(2026, 0, 28));
  const endDate = new Date(Date.UTC(2026, 1, 16));

  const dates = [];
  for (
    let d = new Date(startDate);
    d <= endDate;
    d.setUTCDate(d.getUTCDate() + 1)
  ) {
    if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) {
      dates.push(new Date(d));
    }
  }

  console.log(`Total weekdays to process: ${dates.length}\n`);

  let totalRecords = 0;
  let processedDates = 0;
  let skippedDates = 0;
  let headerLogged = false;

  for (const date of dates) {
    const dateStr = formatDateBSE(date);
    const dbDate = formatDateDB(date);
    process.stdout.write(`${dateStr} ... `);

    try {
      const csvContent = await fetchBSECSV(date);

      if (!csvContent) {
        console.log("no data, skipped");
        skippedDates++;
        await sleep(1000);
        continue;
      }

      let csvStr = csvContent;
      if (csvStr.charCodeAt(0) === 0xfeff) csvStr = csvStr.slice(1);

      const records = parseCSVLib(csvStr, {
        columns: (headers) => headers.map(normalizeHeader),
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        trim: true,
        bom: true,
      });

      if (!headerLogged && records.length > 0) {
        console.log(`\n  CSV columns: ${Object.keys(records[0]).join(", ")}`);
        headerLogged = true;
        process.stdout.write(`${dateStr} ... `);
      }

      if (records.length === 0) {
        console.log("empty CSV, skipped");
        skippedDates++;
        await sleep(1000);
        continue;
      }

      const dbRecords = records.map((r) => ({
        trade_date: dbDate,
        exchange: "BSE",
        security_code: r["Scrip Name"] || null,
        issuer_name: null,
        coupon_rate: null,
        maturity_date: null,
        ltp: r["Close Price"]
          ? parseFloat(String(r["Close Price"]).replace(/,/g, ""))
          : null,
        turnover_rs_lacs: r["Total Trade Turnover (Rs. Lakhs)"]
          ? parseFloat(
              String(r["Total Trade Turnover (Rs. Lakhs)"]).replace(/,/g, ""),
            )
          : null,
        no_of_trades: r["Total Trade Volume"]
          ? parseInt(String(r["Total Trade Volume"]).replace(/,/g, ""))
          : null,
        bond_type: null,
        face_value: null,
        credit_rating: null,
        raw_data: r,
      }));

      // Delete old records for this date, then insert new ones
      await supabase
        .from("bond_trades")
        .delete()
        .eq("trade_date", dbDate)
        .eq("exchange", "BSE");

      const { error } = await supabase.from("bond_trades").insert(dbRecords);
      if (error) throw new Error(`DB insert failed: ${error.message}`);

      console.log(`${dbRecords.length} records saved`);
      totalRecords += dbRecords.length;
      processedDates++;
    } catch (error) {
      console.error(`\nERROR on ${dateStr}: ${error.message}`);
      console.log("Breaking due to error. No notification sent.");
      process.exit(1);
    }

    await sleep(1000);
  }

  // Send a single Telegram notification on success
  const msg = [
    `<b>BSE Data Migration Complete</b>`,
    ``,
    `<b>Range:</b> 01/01/2026 - 16/02/2026`,
    `<b>Dates processed:</b> ${processedDates}`,
    `<b>Dates skipped (no data):</b> ${skippedDates}`,
    `<b>Total records migrated:</b> ${totalRecords}`,
    ``,
    `<i>Completed: ${new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" })}</i>`,
  ].join("\n");

  try {
    await bot.sendMessage(process.env.TELEGRAM_CHANNEL, msg, {
      parse_mode: "HTML",
    });
    console.log("\nTelegram notification sent.");
  } catch (err) {
    console.error("\nFailed to send Telegram:", err.message);
  }

  console.log("\n=== Migration Complete ===");
  console.log(
    `Processed: ${processedDates} dates, ${totalRecords} total records`,
  );
  console.log(`Skipped: ${skippedDates} dates`);
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
