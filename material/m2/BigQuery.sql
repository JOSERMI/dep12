CREATE OR REPLACE TABLE `miprojecto.midataset.offices` (
  `officeCode` STRING NOT NULL,
  `city` STRING NOT NULL,
  `phone` STRING NOT NULL,
  `addressLine1` STRING NOT NULL,
  `addressLine2` STRING,
  `state` STRING,
  `country` STRING NOT NULL,
  `postalCode` STRING NOT NULL,
  `territory` STRING NOT NULL
);

CREATE OR REPLACE TABLE `miprojecto.midataset.employees` (
  `employeeNumber` INT64 NOT NULL,
  `lastName` STRING NOT NULL,
  `firstName` STRING NOT NULL,
  `extension` STRING NOT NULL,
  `email` STRING NOT NULL,
  `officeCode` STRING NOT NULL,
  `reportsTo` FLOAT64,
  `jobTitle` STRING NOT NULL
);

CREATE OR REPLACE TABLE `miprojecto.midataset.customers` (
  `customerNumber` INT64 NOT NULL,
  `customerName` STRING NOT NULL,
  `contactLastName` STRING NOT NULL,
  `contactFirstName` STRING NOT NULL,
  `phone` STRING NOT NULL,
  `addressLine1` STRING NOT NULL,
  `addressLine2` STRING,
  `city` STRING NOT NULL,
  `state` STRING,
  `postalCode` STRING,
  `country` STRING NOT NULL,
  `salesRepEmployeeNumber` FLOAT64,
  `creditLimit` FLOAT64
);

CREATE OR REPLACE TABLE `miprojecto.midataset.orders` (
  `orderNumber` INT64 NOT NULL,
  `orderDate` DATE NOT NULL,
  `requiredDate` DATE NOT NULL,
  `shippedDate` DATE,
  `status` STRING NOT NULL,
  `comments` STRING,
  `customerNumber` INT64 NOT NULL
);

CREATE OR REPLACE TABLE `miprojecto.midataset.payments` (
  `customerNumber` INT64 NOT NULL,
  `checkNumber` STRING NOT NULL,
  `paymentDate` DATE NOT NULL,
  `amount` FLOAT64 NOT NULL
);

CREATE OR REPLACE TABLE `miprojecto.midataset.orderdetails` (
  `orderNumber` INT64 NOT NULL,
  `productCode` STRING NOT NULL,
  `quantityOrdered` INT64 NOT NULL,
  `priceEach` FLOAT64 NOT NULL,
  `orderLineNumber` INT64 NOT NULL
);

CREATE OR REPLACE TABLE `miprojecto.midataset.products` (
  `productCode` STRING NOT NULL,
  `productName` STRING NOT NULL,
  `productLine` STRING NOT NULL,
  `productScale` STRING NOT NULL,
  `productVendor` STRING NOT NULL,
  `productDescription` STRING NOT NULL,
  `quantityInStock` INT64 NOT NULL,
  `buyPrice` FLOAT64 NOT NULL,
  `MSRP` FLOAT64 NOT NULL
);

CREATE OR REPLACE TABLE `miprojecto.midataset.productlines` (
  `productLine` STRING NOT NULL,
  `textDescription` STRING,
  `htmlDescription` STRING,
  `image` STRING
);