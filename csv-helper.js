const fs = require("fs");
const parse = require("csv-parse");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const inputFile = "./archive/min/2019-Dec_v2.csv";
const outputFile = "./archive/min/2019-Dec_v4.csv";

class UserSession {
  constructor(idSession, cantidad) {
    this.idSession = idSession;
    this.cantidad = cantidad;
  }
}

class Row {
  constructor(
    eventTime,
    eventType,
    productId,
    categoryCode,
    brand,
    price,
    userId,
    userSession
  ) {
    this.eventTime = eventTime;
    this.eventType = eventType;
    this.productId = productId;
    this.categoryCode = categoryCode;
    this.brand = brand;
    this.price = price;
    this.userId = userId;
    this.userSession = userSession;
  }
}

var rows = [Row];
var userSessions = [UserSession];

fs.createReadStream(inputFile)
  .pipe(parse())
  .on("data", (row) => {
    // console.log(row);
    rows.push(
      new Row(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
    );
  })
  .on("end", () => {
    rows.sort(function (a, b) {
      return a.cantidad - b.cantidad;
    });
    rows.forEach((row) => {
      // console.log(row);
      let exist = false;
      let i = 0;
      while (i < userSessions.length && exist == false) {
        if (userSessions[i].idSession === row.userSession) {
          exist = true;
          userSessions[i].cantidad++;
        } else {
          i++;
        }
      }

      if (!exist) {
        //find distinct user session!
        userSessions.push(new UserSession(row.userSession, 1));
      }
    });

    let records = [];
    records.push({
      event_time: "event_time",
      event_type: "event_type",
      product_id: "product_id",
      category_code: "category_code",
      brand: "brand",
      price: "price",
      user_id: "user_id",
      user_session: "user_session",
    });

    userSessions.sort(function (a, b) {
      return a.cantidad - b.cantidad;
    });

    userSessions.reverse();

    for (let index = 0; index < userSessions.length; index++) {
      if (userSessions[index].cantidad > 4) {
        console.log(userSessions[index]);
        rows.forEach((r) => {
          if (
            r.userSession === userSessions[index].idSession &&
            records.length != 1000
          ) {
            records.push({
              event_time: r.eventTime,
              event_type: r.eventType,
              product_id: r.productId,
              category_code: r.categoryCode,
              brand: r.brand,
              price: r.price,
              user_id: r.userId,
              user_session: r.userSession,
            });
          }
        });
      }
    }

    const csvWriter = createCsvWriter({
      path: outputFile,
      header: [
        "event_time",
        "event_type",
        "product_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session",
      ],
    });

    records.forEach((r) => console.log(r));

    csvWriter.writeRecords(records).then(() => {
      console.log("...Done");
    });

    console.log("CSV file successfully processed");
  });
