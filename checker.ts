import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()


function problem1() {
  return prisma.$queryRaw`SELECT firstName,
  lastName, income  FROM Customer  WHERE income <= 60000 AND income >= 50000  ORDER BY income DESC, lastName ASC, firstName ASC  LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw`SELECT 
    e.sin,
    b.branchName,
    e.salary,
    CAST((e2.salary - e.salary) AS CHAR(10)) AS "Salary Diff"
  FROM 
    Employee e
  JOIN 
    Branch b ON e.branchNumber = b.branchNumber
  JOIN
    Employee e2 ON e2.sin = b.managerSIN
  WHERE 
    b.branchName IN ('London', 'Berlin')
  ORDER BY 
    e2.salary - e.salary DESC
  LIMIT 10;
  `
}

function problem3() {
  return prisma.$queryRaw`SELECT
    firstName, lastName, income
  FROM
    Customer c
  WHERE
    income >= ALL (
      SELECT income * 2
      FROM Customer
      WHERE lastName = 'Butler'
    )
  ORDER BY
    lastName ASC, firstName ASC
  LIMIT 10;
  `
}

function problem4() {
  return prisma.$queryRaw`SELECT 
    o.customerID, c.income, o.accNumber, a.branchNumber
  FROM
    Customer c
  JOIN
    Owns o ON o.customerID = c.customerID
  JOIN
    Account a ON o.accNumber = a.accNumber
  WHERE
    c.income > 80000
    AND EXISTS (
      SELECT 1
      FROM 
        Owns o2
      JOIN
        Account a2 ON o2.accNumber = a2.accNumber
      JOIN
        Branch b2 ON a2.branchNumber = b2.branchNumber
      WHERE 
        o2.customerID = c.customerID
        AND b2.branchName IN ('London', 'Latveria')
    )
    AND EXISTS (
      SELECT 1
      FROM 
          Owns o3
      JOIN
          Account a3 ON o3.accNumber = a3.accNumber
      JOIN
          Branch b3 ON a3.branchNumber = b3.branchNumber
      WHERE 
          o3.customerID = c.customerID
          AND b3.branchName = 'Latveria'
  )
    ORDER BY 
      c.customerID ASC, o.accNumber ASC
    
    LIMIT 10;  
    `
}

function problem5() {
  return prisma.$queryRaw`SELECT 
      c.customerID, a.type, o.accNumber, a.balance
    FROM 
      Customer c
    JOIN
      Owns o ON o.customerID = c.customerID
    JOIN
      Account a ON o.accNumber = a.accNumber
    WHERE
      o.accNumber IN (
        SELECT accNumber
        FROM Account
        WHERE type IN ('BUS', 'SAV')
      )
    ORDER BY 
      o.customerID ASC, a.type ASC, o.accNumber ASC
    LIMIT 10;
      
    
  ` 
}

function problem6() {
  return prisma.$queryRaw`SELECT
    branchName, accNumber, balance
  FROM
    Account a
  JOIN
    Branch b ON a.branchNumber = b.branchNumber
  JOIN
    Employee e ON b.managerSIN = e.SIN
  WHERE
    a.balance > 100000
    AND e.firstName = 'Phillip'
    AND e.lastName = 'Edwards'
  ORDER BY
    a.accNumber ASC
  LIMIT 10;
    `
}

function problem7() {
  return prisma.$queryRaw`SELECT DISTINCT c.customerID
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  JOIN Branch b ON a.branchNumber = b.branchNumber
  WHERE b.branchName = 'New York'
  AND c.customerID NOT IN (
      SELECT c1.customerID
      FROM Customer c1
      JOIN Owns o1 ON c1.customerID = o1.customerID
      JOIN Account a1 ON o1.accNumber = a1.accNumber
      JOIN Branch b1 ON a1.branchNumber = b1.branchNumber
      WHERE b1.branchName = 'London'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM Owns o2
      JOIN Account a2 ON o2.accNumber = a2.accNumber
      JOIN Customer c2 ON o2.customerID = c2.customerID
      WHERE a2.branchNumber = (
          SELECT branchNumber
          FROM Branch
          WHERE branchName = 'London'
      ) AND c.customerID = c2.customerID
  )
  ORDER BY c.customerID ASC  
  LIMIT 10;`
}

function problem8() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary,
  CASE
      WHEN e.sin = b.managerSIN THEN b.branchName
      ELSE NULL
  END AS branchName
FROM Employee e
LEFT JOIN Branch b ON e.branchNumber = b.branchNumber
WHERE e.salary > 50000
ORDER BY branchName DESC, e.firstName ASC
LIMIT 10;
  `
}

function problem9() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary,
  (SELECT branchName FROM Branch WHERE managerSIN = e.sin) AS branchName
FROM Employee e
WHERE e.salary > 50000
ORDER BY branchName DESC, e.firstName ASC
LIMIT 10;
`
}

function problem10() {
  return prisma.$queryRaw`SELECT c.customerID, c.firstName, c.lastName, c.income
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  WHERE c.income > 5000
  AND NOT EXISTS (
      SELECT b.branchNumber
      FROM Branch b
      JOIN Account a2 ON b.branchNumber = a2.branchNumber
      JOIN Owns o2 ON a2.accNumber = o2.accNumber
      JOIN Customer c2 ON o2.customerID = c2.customerID
      WHERE c2.firstName = 'Helen' AND c2.lastName = 'Morgan'
      AND NOT EXISTS (
          SELECT 1
          FROM Owns o3
          WHERE o3.customerID = c.customerID AND o3.accNumber = a.accNumber
      )
  )
  GROUP BY c.customerID, c.firstName, c.lastName, c.income
  HAVING COUNT(DISTINCT a.branchNumber) = (
      SELECT COUNT(DISTINCT b2.branchNumber)
      FROM Branch b2
      JOIN Account a3 ON b2.branchNumber = a3.branchNumber
      JOIN Owns o3 ON a3.accNumber = o3.accNumber
      JOIN Customer c3 ON o3.customerID = c3.customerID
      WHERE c3.firstName = 'Helen' AND c3.lastName = 'Morgan'
  )
  ORDER BY c.income DESC
  LIMIT 10;`
}

function problem11() {
  return prisma.$queryRaw`SELECT sin, firstName, lastName, salary
  FROM Employee
  WHERE branchNumber = (
      SELECT branchNumber
      FROM Branch
      WHERE branchName = 'Berlin'
  )
  AND salary = (
      SELECT MIN(salary)
      FROM Employee
      WHERE branchNumber = (
          SELECT branchNumber
          FROM Branch
          WHERE branchName = 'Berlin'
      )
  )
  ORDER BY sin ASC;`
}

function problem14() {
  return prisma.$queryRaw`SELECT CAST(SUM(salary) AS CHAR(10)) AS "sum of employees salaries"
  FROM Employee
  WHERE branchNumber = (
      SELECT branchNumber
      FROM Branch
      WHERE branchName = 'Moscow'
  );
  `
}

function problem15() {
  return prisma.$queryRaw`select * from Customer`
}


function problem17() {
  return prisma.$queryRaw`select * from Customer`
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