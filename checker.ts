import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`select firstName, lastName, income from Customer where income <= 60000 and income >= 50000 order by income desc, lastName asc, firstName asc LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw`SELECT 
  e.sin,
  b.branchName,
  e.salary,
  CAST((e2.salary - e.salary) AS CHAR(10)) AS \`Salary Diff\`
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
	firstName,
    lastName,
    income
FROM
	Customer
WHERE 
    income >= ALL (SELECT 2 * income FROM Customer WHERE lastName = 'Butler') 
ORDER BY 
    lastName ASC, 
    firstName ASC
LIMIT 10;`
}

function problem4() {
  return prisma.$queryRaw`SELECT 
	c.customerID,
    c.income,
    a.accNumber,
    a.branchNumber
FROM Customer c
JOIN Owns o ON c.customerID = o.customerID
JOIN Account a ON o.accNumber = a.accNumber
JOIN Branch b on b.branchNumber = a.branchNumber
WHERE
	c.income > 80000
    AND EXISTS (
        SELECT 1 FROM Account a1
        JOIN Owns o1 ON a1.accNumber = o1.accNumber
        JOIN Branch b1 ON a1.branchNumber = b1.branchNumber 
        WHERE 
            o1.customerID = o.customerID 
            AND b1.branchName = 'London'
    )
    AND EXISTS (
        SELECT 1 FROM Account a1
        JOIN Owns o1 ON a1.accNumber = o1.accNumber
        JOIN Branch b1 ON a1.branchNumber = b1.branchNumber 
        WHERE 
            o1.customerID = o.customerID 
            AND b1.branchName = 'Latveria'
    )
ORDER BY
	c.customerID ASC,
	a.accNumber ASC
LIMIT 10;
`
}

function problem5() {
  return prisma.$queryRaw`SELECT 
  c.customerID,
  a.type,
  a.accNumber,
  a.balance
FROM Customer c
JOIN Owns o ON c.customerID = o.customerID
JOIN Account a ON o.accNumber = a.accNumber
WHERE a.type IN ('BUS', 'SAV')
  AND (
      EXISTS (
          SELECT 1 FROM Account a1 
          JOIN Owns o1 ON a1.accNumber = o1.accNumber 
          WHERE o1.customerID = c.customerID AND a1.type = 'BUS'
      )
      OR EXISTS (
          SELECT 1 FROM Account a2 
          JOIN Owns o2 ON a2.accNumber = o2.accNumber 
          WHERE o2.customerID = c.customerID AND a2.type = 'SAV'
      )
  )
ORDER BY 
  c.customerID ASC, 
  a.type ASC, 
  a.accNumber ASC
LIMIT 10;`
}

function problem6() {
  return prisma.$queryRaw`SELECT 
  b.branchName,
  a.accNumber,
  a.balance
FROM 
  Branch b
JOIN 
  Account a ON b.branchNumber = a.branchNumber
JOIN 
  Employee e ON b.managerSIN = e.sin
WHERE 
  a.balance > 100000
  AND e.firstName = 'Phillip' 
  AND e.lastName = 'Edwards'
ORDER BY 
  a.accNumber ASC
LIMIT 10;`
}

function problem7() {
  return prisma.$queryRaw`SELECT DISTINCT c.customerID
  FROM Customer c
  JOIN Owns o1 ON c.customerID = o1.customerID
  JOIN Account a1 ON o1.accNumber = a1.accNumber
  RIGHT JOIN Branch b1 ON a1.branchNumber = b1.branchNumber AND b1.branchName = 'New York'
  WHERE 
      c.customerID NOT IN (
          SELECT DISTINCT c2.customerID
          FROM Customer c2
          JOIN Owns o2 ON c2.customerID = o2.customerID
          JOIN Account a2 ON o2.accNumber = a2.accNumber
          INNER JOIN Branch b2 ON a2.branchNumber = b2.branchNumber AND b2.branchName = 'London'
      )
      AND c.customerID NOT IN (
          SELECT DISTINCT c3.customerID
          FROM Customer c3
          JOIN Owns o3 ON c3.customerID = o3.customerID
          INNER JOIN Owns o4 ON o3.accNumber = o4.accNumber AND o3.customerID != o4.customerID
          JOIN Owns o5 ON o4.customerID = o5.customerID
          JOIN Account a3 ON o5.accNumber = a3.accNumber
          INNER JOIN Branch b3 ON a3.branchNumber = b3.branchNumber AND b3.branchName = 'London'
      )
  ORDER BY c.customerID ASC
  LIMIT 10;
  `
}

function problem8() {
  return prisma.$queryRaw`SELECT 
  e.sin,
  e.firstName,
  e.lastName,
  e.salary,
  IF(e.sin = b.managerSIN, b.branchName, NULL) AS branchName
FROM 
  Employee e
LEFT OUTER JOIN 
  Branch b ON e.branchNumber = b.branchNumber
WHERE 
  e.salary > 50000
ORDER BY 
  branchName DESC,
  e.firstName ASC
LIMIT 10;`
}

function problem9() {
  return prisma.$queryRaw`SELECT 
  e.sin,
  e.firstName,
  e.lastName,
  e.salary,
  CASE 
      WHEN e.sin = b.managerSIN THEN b.branchName
      ELSE NULL
  END AS branchName
FROM 
  Employee e,
  Branch b
WHERE 
  e.salary > 50000
  AND e.branchNumber = b.branchNumber
ORDER BY 
  branchName DESC,
  e.firstName ASC
LIMIT 10;
`
}

function problem10() {
  return prisma.$queryRaw`SELECT c.customerID, c.firstName, c.lastName, c.income
  FROM Customer c
  WHERE c.income > 5000
  AND NOT EXISTS (
      SELECT b.branchNumber
      FROM Branch b
      WHERE NOT EXISTS (
          SELECT 1
          FROM Owns o
          JOIN Account a ON o.accNumber = a.accNumber
          WHERE o.customerID = c.customerID
          AND a.branchNumber = b.branchNumber
      )
      AND EXISTS (
          SELECT 1
          FROM Owns o2
          JOIN Account a2 ON o2.accNumber = a2.accNumber
          JOIN Customer c2 ON o2.customerID = c2.customerID
          WHERE c2.firstName = 'Helen' AND c2.lastName = 'Morgan'
          AND b.branchNumber = a2.branchNumber
      )
  )
  ORDER BY c.income DESC
  LIMIT 10;
  `
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
  ORDER BY SIN ASC;
  `
}

function problem14() {
  return prisma.$queryRaw`SELECT CAST(SUM(salary) AS CHAR(10)) AS \`sum of employees salaries\`
  FROM Employee
  WHERE branchNumber = (
      SELECT branchNumber
      FROM Branch
      WHERE branchName = 'Moscow'
  );`
}

function problem15() {
  return prisma.$queryRaw`SELECT c.customerID, c.firstName, c.lastName
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  GROUP BY c.customerID
  HAVING COUNT(DISTINCT a.branchNumber) = 4
  ORDER BY c.lastName ASC, c.firstName ASC
  LIMIT 10;`
}


function problem17() {
  return prisma.$queryRaw`SELECT 
  c.customerID,
  c.firstName,
  c.lastName,
  c.income,
  AVG(a.balance) AS \`average account balance\`
FROM 
  Customer c
JOIN 
  Owns o ON c.customerID = o.customerID
JOIN 
  Account a ON o.accNumber = a.accNumber
WHERE 
  c.lastName LIKE 'S%' AND c.lastName LIKE '%e%' 
GROUP BY 
  c.customerID
HAVING 
  COUNT(o.accNumber) >= 3
ORDER BY 
  c.customerID ASC
LIMIT 10;`
}

function problem18() {
  return prisma.$queryRaw`SELECT 
  a.accNumber,
  a.balance,
  SUM(t.amount) AS \`sum of transaction amounts\`
FROM 
  Account a
JOIN 
  Transactions t ON a.accNumber = t.accNumber
JOIN 
  Branch b ON a.branchNumber = b.branchNumber
WHERE 
  b.branchName = 'Berlin'
GROUP BY 
  a.accNumber
HAVING 
  COUNT(t.transNumber) >= 10
ORDER BY 
  \`sum of transaction amounts\` ASC
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
    // if (i === 11) console.log(result);
    // if (i === 11) console.log(answer);
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