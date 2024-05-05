import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`SELECT firstName, lastName, income
  FROM Customer
  WHERE income BETWEEN 50000 AND 60000
  ORDER BY income DESC, lastName ASC, firstName ASC
  LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw`select e.sin, b.branchName, e.salary, CAST((m.salary - e.salary) AS CHAR(10)) AS "Salary Diff"
  from Employee e
  join Branch b ON e.branchNumber = b.branchNumber
  join Employee m ON b.managerSIN = m.sin
  where b.branchName in ('London', 'Berlin')
  order by m.salary - e.salary DESC
  LIMIT 10;
  `
}

function problem3() {
  return prisma.$queryRaw`select firstName, lastName, income
  FROM Customer c
  WHERE income >= 2 * (
    SELECT MAX(income)
    FROM Customer
    WHERE lastName = 'Butler'
  )
  ORDER BY lastName ASC, firstName ASC
  LIMIT 10;
  `
}

function problem4() {
  return prisma.$queryRaw`select c.customerID, income, a.accNumber, b.branchNumber
  from Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o. accNumber = a.accNumber
  JOIN Branch b ON b.branchNumber = a.branchNumber
  WHERE income > 80000
  AND EXISTS (
    SELECT 1
    FROM Owns o1
    JOIN Account a1 ON o1.accNumber = a1.accNumber
    JOIN Branch b1 ON a1.branchNumber = b1.branchNumber
    WHERE b1.branchName = 'London'
    AND o1.customerID = c.customerID
  )
  AND EXISTS (
    SELECT 1
    FROM Owns o2
    JOIN Account a2 ON o2.accNumber = a2.accNumber
    JOIN Branch b2 ON a2.branchNumber = b2.branchNumber
    WHERE b2.branchName = 'Latveria'
    AND o2.customerID = c.customerID
  )
  ORDER BY c.customerID ASC, o.accNumber ASC
  LIMIT 10;
  `
}

function problem5() {
  return prisma.$queryRaw`select c.customerID, a.type, o.accNumber, a.balance
  from Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o. accNumber = a.accNumber
  WHERE a.type IN ('BUS', 'SAV')
  ORDER BY c.customerID ASC, a.type ASC, accNumber ASC
  LIMIT 10;`
}

function problem6() {
  return prisma.$queryRaw`select b.branchName, a.accNumber, a.balance
  from Account as a
  join Branch as b ON b.branchNumber = a.branchNumber
  where a.balance > 100000
  and b.managerSIN = (
    SELECT sin
    from Employee where firstname = 'Phillip'
  )
  order by a.accNumber asc
  LIMIT 10;
  `
}

function problem7() {
  return prisma.$queryRaw`select c.customerID
  from Customer c
  join Owns o on c.customerID = o.customerID
  join Account a on o.accNumber = a.accNumber
  join Branch b on b.branchNumber = a.branchNumber
  where b.branchName = 'New York'
  and c.customerID not in (
    select o.customerID
    from Owns o
    join Account a on o.accNumber = a.accNumber
    join Branch b on b.branchNumber = a.branchNumber
    where b.branchName = 'London'
  )
  and c.customerID not in (
    select DISTINCT o1.customerID
    from Owns o1
    join Owns o2 on o1.accNumber = o2.accNumber and o1.customerID != o2.customerID
    where o2.customerID in (
      select o.customerID
      from Owns o
      join Account a on o.accNumber = a.accNumber
      join Branch b on b.branchNumber = a.branchNumber
      where b.branchName = 'London'
    )
  )
  order by c.customerID asc
  LIMIT 10;
  `
}

function problem8() {
  return prisma.$queryRaw`
  SELECT e.sin, e.firstName, e.lastName, e.salary,
    CASE
    WHEN e.sin IN (SELECT managerSIN FROM Branch) THEN b.branchName
    ELSE NULL
    END AS branchName
  FROM Employee e
  LEFT JOIN Branch b ON b.branchNumber = e.branchNumber
  WHERE e.salary > 50000
  ORDER BY branchName DESC, e.firstName ASC
  LIMIT 10;`
}

function problem9() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary,
  CASE WHEN 
  EXISTS (
    SELECT 1 FROM Branch b WHERE b.managerSIN = e.SIN
  ) THEN (
    SELECT b.branchName FROM Branch b WHERE b.managerSIN = e.SIN
  ) ELSE 
    NULL END AS branchName
  FROM Employee e
  WHERE e.salary > 50000
  ORDER BY branchName DESC, e.firstName ASC
  LIMIT 10;`
}

function problem10() {
  return prisma.$queryRaw`
  SELECT DISTINCT c.customerID, c.firstName, c.lastName, c.income
  FROM Customer c
  JOIN Owns o on c.customerID = o.customerID
  JOIN Account a on o.accNumber = a.accNumber
  WHERE c.income > 5000
ORDER BY c.income DESC
LIMIT 10;
`
}

function problem11() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary
  FROM Employee e
  JOIN Branch b on b.branchNumber = e.branchNumber
  WHERE b.branchName = 'Berlin' 
    AND e.salary = (
      SELECT MIN(e.salary)
      FROM Employee e
      JOIN Branch b on e.branchNumber = b.branchNumber
      WHERE b.branchName = 'Berlin'
      )
  ORDER BY sin ASC
  LIMIT 10;`
}

function problem14() {
  return prisma.$queryRaw`
  select CAST(SUM(e.salary) AS CHAR(10)) as "sum of employees salaries"
  from Employee e
  join Branch b on b.branchNumber = e.branchNumber
  where b.branchName = 'Moscow'
  LIMIT 10;
  `
}

function problem15() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.firstName, c.lastName
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  JOIN Branch b ON b.branchNumber = a.branchNumber
  GROUP BY o.customerID
  HAVING COUNT(DISTINCT b.branchName) = 4
  ORDER BY c.lastName ASC, c.firstName ASC;
  `
}


function problem17() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.firstName, c.lastName, c.income, AVG(a.balance) AS "average account balance"
  FROM Customer c
  JOIN Owns o ON c.customerID = o.customerID
  JOIN Account a ON o.accNumber = a.accNumber
  WHERE c.lastName LIKE 'S%' AND c.lastName LIKE '%e%'
  GROUP BY o.customerID
  HAVING COUNT(DISTINCT o.accNumber) >= 3
  ORDER BY o.customerID ASC
  LIMIT 10;
  `
}

function problem18() {
  return prisma.$queryRaw`SELECT a.accNumber, a.balance, SUM(t.amount) as "sum of transaction amounts"
  FROM Account a
  JOIN Transactions t on t.accNumber = a.accNumber
  JOIN Branch b on b.branchNumber = a.branchNumber
  WHERE b.branchName = 'Berlin'
  GROUP by a.accNumber
  HAVING COUNT(t.transNumber) >= 10
  ORDER by SUM(t.amount) ASC
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