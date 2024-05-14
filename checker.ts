import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()

//bank.txt 파일은 MySQL 데이터베이스의 데이터를 내보내는 SQL 덤프 파일으로, customer, employee, transcations 등 다양한 table들과 정보를 담고 있습니다.


function problem1() {
  return prisma.$queryRaw`select firstName, lastName, income from Customer where income <= 60000 and income >= 50000 order by income desc, lastName asc, firstName asc LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw`
  SELECT E.sin, B.branchName, E.salary, CAST(M.salary - E.salary AS CHAR(5)) AS 'Salary Diff'
  FROM (
    SELECT E.sin, B.branchNumber
    FROM Employee E
    JOIN Branch B ON E.branchNumber = B.branchNumber
    WHERE B.branchName IN ('London', 'Berlin')
  ) AS Temp
  JOIN Employee E ON Temp.sin = E.sin
  JOIN Branch B ON Temp.branchNumber = B.branchNumber
  JOIN Employee M ON B.managerSIN = M.sin
  ORDER BY (M.salary - E.salary) DESC
  LIMIT 10;`}

function problem3() {
  return prisma.$queryRaw`
    SELECT firstName, lastName, income
    FROM Customer
    WHERE income >= ALL(
      SELECT income * 2
      FROM Customer
      WHERE lastName = 'Butler'
    )
    ORDER BY lastName, firstName
    LIMIT 10;`}

function problem4() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.income, o.accNumber, a.branchNumber
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  JOIN Branch b ON a.branchNumber = b.branchNumber
  WHERE c.income > 80000
    AND EXISTS (
      SELECT 1
      FROM Owns o1
      JOIN Account a1 ON o1.accNumber = a1.accNumber
      JOIN Branch b1 ON a1.branchNumber = b1.branchNumber
      WHERE b1.branchName = 'London' AND o1.customerID = c.customerID
    )
    AND EXISTS (
      SELECT 1
      FROM Owns o2
      JOIN Account a2 ON o2.accNumber = a2.accNumber
      JOIN Branch b2 ON a2.branchNumber = b2.branchNumber
      WHERE b2.branchName = 'Latveria' AND o2.customerID = c.customerID
    )
  ORDER BY c.customerID, o.accNumber
  LIMIT 10;`}

function problem5() {
  return prisma.$queryRaw`
  SELECT c.customerID, a.type, a.accNumber, a.balance
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  WHERE a.type IN ('BUS', 'SAV')
  ORDER BY c.customerID, a.type, a.accNumber
  LIMIT 10;`;}

function problem6() {
  return prisma.$queryRaw`
  SELECT b.branchName, a.accNumber, a.balance
  FROM Branch b
  JOIN Employee e ON e.branchNumber = b.branchNumber
  JOIN Account a ON a.branchNumber = b.branchNumber
  WHERE a.balance > 100000
    AND e.firstName = 'Phillip'
    AND e.lastName = 'Edwards'
  ORDER BY a.accNumber
  LIMIT 10;`;}


function problem7() {
  return prisma.$queryRaw`
  SELECT c.customerID
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  WHERE c.customerID = ANY(
    SELECT c.customerID
    FROM Customer c
    JOIN Owns o ON c.customerID = o.customerID
    JOIN Account a ON o.accNumber = a.accNumber
    JOIN Branch b ON a.branchNumber = b.branchNumber
    WHERE b.branchName = 'New York'
  )
  AND c.customerID != ALL(
    SELECT c.customerID
    FROM Customer c
    JOIN Owns o ON c.customerID = o.customerID
    JOIN Account a ON o.accNumber = a.accNumber
    WHERE a.accNumber = ANY(
      SELECT a.accNumber
      FROM Customer c
      JOIN Owns o ON c.customerID = o.customerID
      JOIN Account a ON o.accNumber = a.accNumber
      WHERE c.customerID = ANY(
        SELECT c.customerID
        FROM Customer c
        JOIN Owns o ON c.customerID = o.customerID
        JOIN Account a ON o.accNumber = a.accNumber
        JOIN Branch b ON a.branchNumber = b.branchNumber
        WHERE b.branchName = 'London'
      )
    )
  )
  GROUP BY c.customerID
  ORDER BY c.customerID
  LIMIT 10;`}

function problem8() {
  return prisma.$queryRaw`
  SELECT e.sin, e.firstName, e.lastName, e.salary, b.branchName
  FROM Employee e
  LEFT JOIN Branch b ON e.sin = b.managerSIN
  WHERE e.salary > 50000
  ORDER BY b.branchName DESC, e.firstName
  LIMIT 10;`}

function problem9() {
  return prisma.$queryRaw`
  SELECT e.sin, e.firstName, e.lastName, e.salary, 
         CASE WHEN e.sin = b.managerSIN THEN b.branchName END AS 'branchName'
  FROM Employee e, Branch b
  WHERE e.salary > 50000 
    AND e.branchNumber = b.branchNumber
  ORDER BY branchName DESC, e.firstName
  LIMIT 10;`}

function problem10() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.firstName, c.lastName, c.income
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  WHERE c.income > 5000
    AND a.branchNumber IN (
      SELECT a1.branchNumber
      FROM Customer c1
      JOIN Owns o1 ON c1.customerID = o1.customerID
      JOIN Account a1 ON o1.accNumber = a1.accNumber
      WHERE c1.firstName = 'Helen' AND c1.lastName = 'Morgan'
    )
  GROUP BY c.customerID
  HAVING COUNT(DISTINCT a.branchNumber) = (
    SELECT COUNT(DISTINCT a2.branchNumber)
    FROM Customer c2
    JOIN Owns o2 ON c2.customerID = o2.customerID
    JOIN Account a2 ON o2.accNumber = a2.accNumber
    WHERE c2.firstName = 'Helen' AND c2.lastName = 'Morgan'
  )
  ORDER BY c.income DESC
  LIMIT 10;`}

function problem11() {
  return prisma.$queryRaw`
  SELECT e.sin, e.firstName, e.lastName, e.salary
  FROM Employee e
  WHERE e.salary = (
    SELECT MIN(salary)
    FROM Employee
  )
  ORDER BY e.sin
  LIMIT 10;`}

function problem14() {
  return prisma.$queryRaw`
  SELECT CAST(SUM(e.salary) AS CHAR(6)) AS 'sum of employees salaries'
  FROM Employee e
  JOIN Branch b ON e.branchNumber = b.branchNumber
  WHERE b.branchName = 'Moscow';`}

function problem15() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.firstName, c.lastName
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  JOIN Branch b ON a.branchNumber = b.branchNumber
  GROUP BY c.customerID
  HAVING COUNT(DISTINCT b.branchNumber) = 4
  ORDER BY MIN(c.lastName), MIN(c.firstName)
  LIMIT 10;`}


function problem17() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.firstName, c.lastName, c.income, AVG(a.balance) AS 'average account balance'
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  WHERE c.lastName REGEXP 'S.*e.*'
  GROUP BY c.customerID
  HAVING COUNT(o.accNumber) >= 3
  ORDER BY c.customerID
  LIMIT 10;`}

function problem18() {
  return prisma.$queryRaw`
  SELECT a.accNumber, a.balance, SUM(t.amount) AS 'sum of transaction amounts'
  FROM Account a
  JOIN Transactions t ON t.accNumber = a.accNumber
  JOIN Branch b ON b.branchNumber = a.branchNumber
  WHERE b.branchName = 'Berlin'
  GROUP BY a.accNumber
  HAVING COUNT(t.transNumber) >= 10
  ORDER BY SUM(t.amount)
  LIMIT 10;`}

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