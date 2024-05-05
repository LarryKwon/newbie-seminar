import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`
  SELECT firstName, lastName, income
  FROM Customer
  WHERE income BETWEEN 50000 AND 60000
  ORDER BY income DESC, lastName ASC, firstName ASC
  LIMIT 10; 
  `
}

function problem2() {
  return prisma.$queryRaw`
  WITH E AS (
    SELECT E.sin, B.branchName, E.salary, B.managerSIN
    FROM Employee E
    JOIN Branch B ON B.branchNumber = E.branchNumber
    WHERE branchName = 'Berlin' OR branchName = 'London'
  )
  SELECT E.sin, E.branchName, E.salary, CAST((M.salary - E.salary) AS CHAR) AS \`Salary Diff\`
  FROM E
  LEFT JOIN Employee M ON E.managerSIN = M.sin
  ORDER BY M.salary - E.salary DESC
  LIMIT 10;
  `
}

function problem3() {
  return prisma.$queryRaw`
  SELECT firstName, lastName, income
  FROM Customer
  WHERE income >= 2 * (SELECT MAX(income) FROM Customer WHERE lastName = 'Butler')
  ORDER BY lastName ASC, firstName ASC
  LIMIT 10;
  `
}

function problem4() {
  return prisma.$queryRaw`
  WITH QualifiedCustomers AS (
    SELECT DISTINCT C.customerID
    FROM Customer C
    JOIN Owns O ON C.customerID = O.customerID
    JOIN Account A ON O.accNumber = A.accNumber
    WHERE C.income > 80000
      AND A.branchNumber IN (
          SELECT branchNumber
          FROM Branch
          WHERE branchName IN ('London', 'Latveria')
      )
    GROUP BY C.customerID
    HAVING COUNT(DISTINCT A.branchNumber) = 2 -- Ensures the customer has accounts in both branches
  )
    SELECT C.customerID, C.income, O.accNumber, A.branchNumber
    FROM Customer C
    JOIN Owns O ON C.customerID = O.customerID
    JOIN Account A ON O.accNumber = A.accNumber
    WHERE C.customerID IN (SELECT customerID FROM QualifiedCustomers)
    LIMIT 10;
  `
}

function problem5() {
  return prisma.$queryRaw`
    SELECT C.customerID, A.type, A.accNumber, A.balance
    FROM Customer C
    JOIN Owns O ON C.customerID = O.customerID
    JOIN Account A ON O.accNUmber = A.accNumber
    WHERE A.type IN ('BUS', 'SAV')
    ORDER BY C.customerID, A.type, A.accNumber
    LIMIT 10
  `
}

function problem6() {
  return prisma.$queryRaw`
  SELECT B.branchName, A.accNumber, A.balance
  FROM Branch B
  JOIN Account A
  WHERE A.balance > 100000 AND B.managerSIN IN (
          SELECT SIN
          FROM Employee
          WHERE firstName = 'Phillip' AND lastName = 'Edwards'
  )
  ORDER BY A.accNumber
  LIMIT 10;
  `
}

function problem7() {
  return prisma.$queryRaw`
  WITH NewYorkAccounts AS (
    SELECT DISTINCT O.customerID
    FROM Owns O
    JOIN Account A ON O.accNumber = A.accNumber
    JOIN Branch B ON A.branchNumber = B.branchNumber
    WHERE B.branchName = 'New York'
  ),
  LondonAccounts AS (
      SELECT DISTINCT O.customerID
      FROM Owns O
      JOIN Account A ON O.accNumber = A.accNumber
      JOIN Branch B ON A.branchNumber = B.branchNumber
      WHERE B.branchName = 'London'
  ),
  CustomersWithLondonCoOwners AS (
      SELECT DISTINCT O1.customerID
      FROM Owns O1
      JOIN Owns O2 ON O1.accNumber = O2.accNumber AND O1.customerID != O2.customerID
      WHERE O2.customerID IN (SELECT customerID FROM LondonAccounts)
  )
  SELECT customerID
  FROM NewYorkAccounts
  WHERE customerID NOT IN (SELECT customerID FROM LondonAccounts)
    AND customerID NOT IN (SELECT customerID FROM CustomersWithLondonCoOwners)
  ORDER BY customerID ASC
  LIMIT 10;
  `
}

function problem8() {
  return prisma.$queryRaw`
  SELECT E.sin, E.firstName, E.lastName, E.salary, B.branchName
  FROM Employee E
  LEFT JOIN Branch B ON E.sin = B.managerSIN
  WHERE E.salary > 50000
  ORDER BY branchName DESC, firstName ASC
  LIMIT 10;
  `
}

function problem9() {
  return prisma.$queryRaw`select * from Customer`
}

function problem10() {
  return prisma.$queryRaw`
  WITH P as (
    SELECT DISTINCT A.branchNumber
    FROM Customer C
    JOIN Owns O ON C.customerID = O.customerID
    JOIN Account A ON O.accNumber = A.accNumber
    WHERE C.firstName = 'Helen' AND C.lastName = 'Morgan'
  ),
  COUNTP as (
      SELECT COUNT(*) AS COUNT FROM P
  )
  SELECT C.customerID, C.firstName, C.lastName, C.income
  FROM Customer C
  JOIN Owns O ON C.customerID = O.customerID
  JOIN Account A ON O.accNumber = A.accNumber
  WHERE C.income > 5000 AND A.branchNumber IN (SELECT branchNumber FROM P)
  GROUP BY C.customerID
  HAVING COUNT(DISTINCT A.branchNumber) >= (SELECT * FROM COUNTP)
  ORDER BY C.income DESC
  LIMIT 10;
  `
}

function problem11() {
  return prisma.$queryRaw`
  SELECT sin, firstName, lastName, salary
  FROM Employee E
  WHERE branchNumber IN (SELECT branchNumber FROM Branch WHERE branchName = 'Berlin')
  ORDER BY salary
  LIMIT 1;
  `
}

function problem14() {
  return prisma.$queryRaw`
  SELECT CAST(SUM(Employee.salary) AS CHAR) AS 'sum of employees salaries'
  FROM Employee
  JOIN Branch ON Employee.branchNumber = Branch.branchNumber
  WHERE branchName = 'Moscow';
  `
}

function problem15() {
  return prisma.$queryRaw`
  SELECT C.customerID, C.firstName, C.lastName
  FROM Customer C
  JOIN Owns O ON C.customerID = O.customerID
  JOIN Account A ON O.accNumber = A.accNumber
  GROUP BY C.customerID
  HAVING COUNT(DISTINCT A.branchNumber) = 4
  ORDER BY C.lastName, C.firstName;
  `
}


function problem17() {
  return prisma.$queryRaw`
  SELECT C.customerID, C.firstName, C.lastName, C.income, AVG(A.balance) AS 'average account balance'
  FROM Customer C
  JOIN Owns O ON C.customerID = O.customerID
  JOIN Account A ON O.accNumber = A.accNumber
  WHERE C.lastName LIKE 'S%e%'
  GROUP BY C.customerID
  HAVING COUNT(DISTINCT O.accNumber) >=3
  ORDER BY C.customerID;
  `
}

function problem18() {
  return prisma.$queryRaw`
  SELECT Account.accNumber, Account.balance, SUM(Transactions.amount) AS "sum of transaction amounts"
  FROM Account
  JOIN Transactions ON Account.accNumber = Transactions.accNumber
  WHERE  branchnumber IN (SELECT branchNumber FROM Branch WHERE branchName = 'Berlin')
  GROUP BY Account.accNumber
  HAVING COUNT(Transactions.transNumber) >= 10
  ORDER BY SUM(Transactions.amount)
  LIMIT 10;
  `
}

const ProblemList = [
  problem1, problem2, problem3, problem4, problem5, problem6, problem7, problem8, problem9, problem10,
  problem11, problem14, problem15, problem17, problem18
]


async function main() {
  for (let i = 0; i < ProblemList.length; i++) {
    const result = await ProblemList[i]()
    const answer =  JSON.parse(fs.readFileSync(`${ProblemList[i].name}.json`,'utf-8'));
    lodash.isEqual(result, answer) ? console.log(`${ProblemList[i].name}: Correct`) : console.log(`${ProblemList[i].name}: Incorrect`)
  }
}

main()
  .then(async () => {
    await prisma.$disconnect()
  })
  .catch(async (e) => {
    console.error(e)
    await prisma.$disconnect()
    process.exit(1)
  })