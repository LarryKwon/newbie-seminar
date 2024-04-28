import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`select firstName, lastName, income from Customer where income <= 60000 and income >= 50000 order by income desc, lastName asc, firstName asc LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw
  `select * from Customer`
}

function problem3() {
  return prisma.$queryRaw`SELECT firstName, lastName, income
  FROM Customer
  WHERE income >= ALL (SELECT 2 * income FROM Customer WHERE lastName = 'Butler')
  ORDER BY lastName ASC, firstName ASC
  LIMIT 10;`
}

function problem4() {
  return prisma.$queryRaw`SELECT DISTINCT O.customerID, C.income, O.accNumber, A.branchNumber
  FROM Owns O
           JOIN Customer C ON O.customerID = C.customerID
           JOIN Account A ON O.accNumber = A.accNumber
  WHERE C.income > 80000
    AND EXISTS (SELECT 1 FROM Owns O1 JOIN Account A1 ON O1.accNumber = A1.accNumber JOIN Branch B1 ON A1.branchNumber = B1.branchNumber WHERE O1.customerID = O.customerID AND B1.branchName = 'London')
    AND EXISTS (SELECT 1 FROM Owns O2 JOIN Account A2 ON O2.accNumber = A2.accNumber JOIN Branch B2 ON A2.branchNumber = B2.branchNumber WHERE O2.customerID = O.customerID AND B2.branchName = 'Latveria')
  ORDER BY O.customerID ASC, O.accNumber ASC
  LIMIT 10;
  `
}

function problem5() {
  return prisma.$queryRaw`SELECT O.customerID, A.type, O.accNumber, A.balance
  FROM Owns O
  JOIN Account A ON O.accNumber = A.accNumber
  WHERE A.type IN ('BUS', 'SAV')
  AND EXISTS (
      SELECT 1 FROM Owns O1 JOIN Account A1 ON O1.accNumber = A1.accNumber WHERE O1.customerID = O.customerID AND A1.type = A.type
  )
  ORDER BY O.customerID ASC, A.type ASC, O.accNumber ASC
  LIMIT 10;`
}

function problem6() {
  return prisma.$queryRaw`SELECT B.branchName, A.accNumber, A.balance
  FROM Account A
  JOIN Branch B ON A.branchNumber = B.branchNumber
  JOIN Employee E ON B.managerSIN = E.sin
  WHERE A.balance > 100000
  AND E.firstName = 'Phillip'
  AND E.lastName = 'Edwards'
  ORDER BY A.accNumber
  LIMIT 10;`
}

function problem7() {
  return prisma.$queryRaw`select * from Customer`
}

function problem8() {
  return prisma.$queryRaw`
  SELECT E.sin, E.firstName, E.lastName, E.salary,
       CASE
           WHEN E.sin IN (SELECT managerSIN FROM Branch) THEN B.branchName
           ELSE NULL
           END AS branchName
FROM Employee E
         LEFT JOIN Branch B ON E.branchNumber = B.branchNumber
WHERE E.salary > 50000
ORDER BY branchName DESC, E.firstName ASC
LIMIT 10;
`
}

function problem9() {
  return prisma.$queryRaw`SELECT E.sin, E.firstName, E.lastName, E.salary,
  (SELECT branchName FROM Branch WHERE managerSIN = E.sin) AS branchName
FROM Employee E
WHERE E.salary > 50000
ORDER BY IF(branchName IS NULL, 1, 0), branchName DESC, E.firstName ASC
LIMIT 10;
`
}

function problem10() {
  return prisma.$queryRaw`select * from Customer`
}

function problem11() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary
  FROM Employee e JOIN Branch b on e.branchNumber = b.branchNumber
  WHERE b.branchName = 'Berlin' AND e.salary = (
      SELECT MIN(e.salary)
      FROM Employee e JOIN Branch b on e.branchNumber = b.branchNumber
      WHERE b.branchName = 'Berlin'
      )`
}

function problem14() {
  return prisma.$queryRaw`
  SELECT SUM(salary) AS sum_salary
  FROM Employee e JOIN Branch b ON e.branchNumber = b.branchNumber
  WHERE b.branchName = 'Moscow'
  `
}

function problem15() {
  return prisma.$queryRaw`SELECT c.customerID, c.firstName, c.lastName
  FROM Customer c
           JOIN Owns o ON c.customerID = o.customerID
           JOIN Account a ON o.accNumber = a.accNumber
           JOIN Branch b ON a.branchNumber = b.branchNumber
  GROUP BY o.customerID
  HAVING COUNT(DISTINCT b.branchName) = 4
  ORDER BY c.lastName ASC, c.firstName ASC;
  `
}


function problem17() {
  return prisma.$queryRaw`SELECT c.customerID, c.firstName, c.lastName, c.income, AVG(a.balance) AS average_account_balance
  FROM Customer c
           JOIN Owns o ON c.customerID = o.customerID
           JOIN Account a ON o.accNumber = a.accNumber
  WHERE c.lastName LIKE 'S%' AND c.lastName LIKE '%e%'
  GROUP BY o.customerID
  HAVING COUNT(DISTINCT o.accNumber) >= 3
  ORDER BY o.customerID ASC;
  `
}

function problem18() {
  return prisma.$queryRaw`select * from Customer`
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