import https from "https";
import http from "http";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import { parse as parseCSVLib } from "csv-parse/sync";

dotenv.config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

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

    req.on("error", reject);

    if (postData) {
      req.write(postData);
    }

    req.end();
  });
}

function normalizeHeader(h) {
  return h.replace(/\n/g, " ").replace(/\s+/g, " ").trim();
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

function formatDateBSEApi(date) {
  const day = String(date.getUTCDate()).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const year = date.getUTCFullYear();

  return `${year}${month}${day}`;
}

function formatDateDB(date) {
  const day = String(date.getUTCDate()).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const year = date.getUTCFullYear();

  return `${year}-${month}-${day}`;
}

function formatDateDisplay(date) {
  const day = String(date.getUTCDate()).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const year = date.getUTCFullYear();

  return `${day}/${month}/${year}`;
}

async function fetchBSEData(date) {
  const apiDate = formatDateBSEApi(date);

  console.log(`Fetching BSE data for ${formatDateDisplay(date)}...`);

  const response = await makeRequest({
    hostname: "api.bseindia.com",
    path: `/BseIndiaAPI/api/rcds_Download/w?frmDate=${apiDate}&toDate=${apiDate}&type=2`,
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
    throw new Error(`BSE API returned status ${response.statusCode}`);
  }

  const body = response.body.trim();

  if (!body) {
    throw new Error("Empty response");
  }

  if (
    body.startsWith("<!DOCTYPE") ||
    body.startsWith("<html") ||
    body.startsWith("<")
  ) {
    throw new Error("Received HTML instead of data");
  }

  const records = parseTabularText(body);

  if (!records.length) {
    throw new Error("No records found");
  }

  console.log(`Fetched ${records.length} rows`);

  return records;
}

async function storeBSEData(records, tradeDate) {
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

      issuer_name: getRecordValue(r, [
        "Issuer Name",
        "issuerName",
        "issuer_name",
      ]),

      coupon_rate: parseOptionalNumber(
        getRecordValue(r, ["Coupon (%)", "Coupon", "coupon", "coupon_rate"]),
      ),

      maturity_date:
        getRecordValue(r, ["Maturity Date", "maturityDate", "maturity_date"]) ||
        null,

      ltp,

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

  if (error) {
    throw new Error(error.message);
  }

  return dbRecords.length;
}

function getDateRange(start, end) {
  const dates = [];

  const current = new Date(start);

  while (current <= end) {
    const day = current.getUTCDay();

    // Skip Saturday and Sunday
    if (day !== 0 && day !== 6) {
      dates.push(new Date(current));
    }

    current.setUTCDate(current.getUTCDate() + 1);
  }

  return dates;
}

async function migrate() {
  const startDate = new Date(Date.UTC(2026, 3, 25)); // 25 Apr 2026
  const endDate = new Date(Date.UTC(2026, 4, 15)); // 15 May 2026

  const dates = getDateRange(startDate, endDate);

  console.log(`Processing ${dates.length} trading days...\n`);

  let successCount = 0;
  let failureCount = 0;

  for (const date of dates) {
    const tradeDate = formatDateDB(date);

    try {
      console.log(`========== ${tradeDate} ==========`);

      const records = await fetchBSEData(date);

      const inserted = await storeBSEData(records, tradeDate);

      console.log(`Stored ${inserted} records\n`);

      successCount++;
    } catch (err) {
      console.error(`Failed for ${tradeDate}`);
      console.error(err.message);
      console.error("");

      failureCount++;
    }
  }

  console.log("========== SUMMARY ==========");
  console.log(`Success: ${successCount}`);
  console.log(`Failed : ${failureCount}`);
}

migrate().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
