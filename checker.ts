import { PrismaClient } from "@prisma/client";
import fs from "fs";
import lodash, { first, orderBy } from "lodash";

const prisma = new PrismaClient();

function problem1() {
  return prisma.customer.findMany({
    select: {
      firstName: true,
      lastName: true,
      income: true,
    },

    where: {
      income: {
        gte: 50000,
        lte: 60000,
      },
    },
    orderBy: [{ income: "desc" }, { lastName: "asc" }, { firstName: "asc" }],
    take: 10,
  });

  return prisma.$queryRaw`select firstName, lastName, income from Customer where income >= 50000 and income <= 60000 order by income desc, lastName asc, firstName asc LIMIT 10`;
}

function problem2() {
  return prisma.$queryRaw`select sin, branchName, salary, CAST( (select salary from Employee where sin = managerSIN) - salary AS CHAR) as "Salary Diff" 
  from Employee JOIN Branch using(branchNumber)
  where branchName = 'London' or branchName = 'Berlin' 
  order by CAST( (select salary from Employee where sin = managerSIN) - salary AS SIGNED) desc LIMIT 10`;
}

function problem3() {
  return prisma.$queryRaw` SELECT
    firstName, lastName, income
    FROM Customer
    WHERE income >= 2 * (
      SELECT MAX(income) from Customer WHERE lastName = 'Butler'
    )
    ORDER BY lastName ASC, firstName ASC
    LIMIT 10
  `;
}

function problem4() {
  return prisma.$queryRaw`
  SELECT
    customerID, income, accNumber, branchNumber
  FROM Customer
  JOIN Owns USING (customerID)
  JOIN Account USING (accNumber)
  WHERE 
    income > 80000
    and customerID IN (
    select customerID from Customer
      JOIN Owns USING (customerID)
        JOIN Account USING (accNumber)
        JOIN Branch USING (branchNumber)
        where branchName = 'London'
    )
    and customerID IN (
    select customerID from Customer
      JOIN Owns USING (customerID)
        JOIN Account USING (accNumber)
        JOIN Branch USING (branchNumber)
        where branchName = 'Latveria'
    )

  ORDER BY customerId ASC, accNumber ASC
  LIMIT 10
  `;
}

function problem5() {
  return prisma.$queryRaw`
  SELECT customerID, type, accNumber, balance
  FROM 
    Account
    JOIN Owns USING (accNumber)
    JOIN Customer USING (customerID)
  WHERE 
    type in ('SAV','BUS')
  ORDER BY customerID ASC, type ASC, accNumber ASC
  LIMIT 10
  `;
} // 어차피 type가 SAV나 BUS를 뽑으니까 customer에 추가조건을 걸지 않아도 됩니다.

function problem6() {
  return prisma.$queryRaw`
  SELECT 
    branchName, accNumber, balance
  FROM
    Account
    JOIN Branch USING (branchNumber)
    JOIN Employee ON Branch.managerSIN = Employee.sin
  WHERE
    balance > 100000
    AND firstName = 'Phillip'
    AND lastName = 'Edwards'
  ORDER BY accNumber ASC
  LIMIT 10
  `;
}

function problem7() {
  return prisma.$queryRaw`
  SELECT
    DISTINCT customerID
  FROM 
    Owns
  WHERE
    customerID IN (
      SELECT customerID
      FROM Owns
        JOIN Account USING (accNumber)
        JOIN Branch USING (branchNumber)
      WHERE
        branchName = 'New York'
    )
    AND customerID NOT IN (
      SELECT customerID
      FROM Owns
        JOIN Account USING (accNumber)
        JOIN Branch USING (branchNumber)
      WHERE
        branchName = 'London'
    )
    AND customerID NOT IN(
      SELECT customerID
      FROM Owns
      WHERE
        accNumber IN(
          SELECT accNumber
          FROM Owns 
          WHERE 
            customerID IN (
              SELECT customerID
                FROM Owns
                  JOIN Account USING (accNumber)
                  JOIN Branch USING (branchNumber)
                WHERE
                  branchName = 'London'
            )
        )
    )
  
  ORDER BY customerID ASC
  LIMIT 10
  `; //.then((res) => console.log(res));
}
// select DISTINCT customerID
// group by customerID
// 둘다 가능합니다.

function problem8() {
  return prisma.$queryRaw`
  SELECT
    sin, firstName, lastName, salary, branchName
  FROM 
    Employee
    LEFT JOIN Branch on Employee.sin = Branch.managerSIN
  WHERE
    salary > 50000
  ORDER BY branchName DESC, firstName ASC
  LIMIT 10
  `;
}

function problem9() {
  return prisma.$queryRaw`
  SELECT
    sin, firstName, lastName, salary,
    CASE
      WHEN sin IN (SELECT managerSIN as sin FROM Branch) THEN (SELECT branchName FROM Branch WHERE managerSIN = sin)
      ELSE NULL
    END AS branchName
  FROM
    Employee
  WHERE salary > 50000
  ORDER BY branchName DESC, firstName ASC
  LIMIT 10
  `;
}

function problem10() {
  return prisma.$queryRaw`
  SELECT
    customerID, firstName, lastName, income
  FROM Customer
  WHERE
    income > 5000
    AND customerID in (
      SELECT customerID
      FROM Owns
        JOIN Account USING (accNumber)
        JOIN Branch USING (branchNumber)
      WHERE
        branchNumber in (
          SELECT branchNumber 
          FROM Account 
          JOIN Owns USING (accNumber)
          JOIN Customer USING (customerID)
          WHERE
            firstName = "Helen"
            AND lastName = "Morgan"
        )
      GROUP BY customerID
      HAVING COUNT(DISTINCT(branchNumber)) >= (
        SELECT COUNT(branchNumber) 
          FROM Account 
          JOIN Owns USING (accNumber)
          JOIN Customer USING (customerID)
          WHERE
            firstName = "Helen"
            AND lastName = "Morgan"
      )
    )
  ORDER BY income DESC
  LIMIT 10
  `; //.then((res) => console.log(res));
}

function problem11() {
  return prisma.$queryRaw`
    SELECT
      sin, firstName, lastName, salary
    FROM Employee
    WHERE
      salary IN (SELECT MIN(salary) from Employee JOIN Branch using (branchNumber) WHERE branchName = 'Berlin' )
    ORDER BY sin ASC
    LIMIT 10
  `;
}

function problem14() {
  return prisma.$queryRaw`
  SELECT
    CAST(sum(salary) as char)  as "sum of employees salaries"
  FROM Employee
    JOIN Branch USING (branchNumber)
  WHERE
    branchName = 'Moscow'
  LIMIT 10
  `; //.then((res) => console.log(res));
}

function problem15() {
  return prisma.$queryRaw`

  SELECT
    customerID, firstName, lastName
  FROM Customer
  WHERE
    customerID IN (
      SELECT customerID
      FROM Owns
        JOIN Account USING (accNumber)
      GROUP BY customerID
      HAVING COUNT(DISTINCT(branchNumber)) = 4
    )
  ORDER BY lastName ASC, firstName ASC
  LIMIT 10  `; //.then((res) => console.log(res));
}

function problem17() {
  return prisma.$queryRaw`
  SELECT
    customerID, firstName, lastName, income, AVG(balance) as "average account balance"
  FROM 
    Customer
    JOIN Owns USING (customerID)
    JOIN Account USING (accNumber)
  WHERE
    lastName LIKE 'S%'
    AND lastName LIKE '%e%'
  GROUP BY customerID
  HAVING count(DISTINCT(accNumber)) > 2
  ORDER BY customerID ASC
  LIMIT 10
  `; //.then((res) => console.log(res));
}

function problem18() {
  return prisma.$queryRaw`
    SELECT
      accNumber, balance, sum(amount) as "sum of transaction amounts"
    FROM Account
      JOIN Transactions USING (accNumber)
      JOIN Branch USING (branchNumber)
    WHERE
      branchName = 'Berlin'
    GROUP BY accNumber
    HAVING count(transNumber) >= 10
    ORDER BY sum(amount) ASC
    LIMIT 10
  `; //.then((res) => console.log(res));
}

const ProblemList = [
  problem1,
  problem2,
  problem3,
  problem4,
  problem5,
  problem6,
  problem7,
  problem8,
  problem9,
  problem10,
  problem11,
  problem14,
  problem15,
  problem17,
  problem18,
];

async function main() {
  for (let i = 0; i < ProblemList.length; i++) {
    const result = await ProblemList[i]();
    const answer = JSON.parse(
      fs.readFileSync(`${ProblemList[i].name}.json`, "utf-8")
    );
    lodash.isEqual(result, answer)
      ? console.log(`${ProblemList[i].name}: Correct`)
      : console.log(`${ProblemList[i].name}: Incorrect`);
  }
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
