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
  return prisma.$queryRaw`select * from Customer`
}

function problem8() {
  return prisma.$queryRaw`select * from Customer`
}

function problem9() {
  return prisma.$queryRaw`select * from Customer`
}

function problem10() {
  return prisma.$queryRaw`select * from Customer`
}

function problem11() {
  return prisma.$queryRaw`select * from Customer`
}

function problem14() {
  return prisma.$queryRaw`select * from Customer`
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