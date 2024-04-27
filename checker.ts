import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`
  select 
    firstName, lastName, income 
  from Customer 
  where 
    income <= 60000 and income >= 50000 
  order by 
    income desc, lastName asc, firstName asc 
  LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw`
  SELECT
    e.sin AS "sin",
    (SELECT branchName
      FROM Branch
      WHERE Branch.branchNumber = e.branchNumber) AS "branchName",
    e.salary AS "salary",
    CAST((
      SELECT man.salary
      FROM Employee AS man
      WHERE man.sin = (
        SELECT managerSIN
        FROM Branch
        WHERE e.branchNumber = Branch.branchNumber
      )
    ) - e.salary AS CHAR(5)) AS "Salary Diff"
  FROM
      Employee AS e
  WHERE
      (SELECT branchName
      FROM Branch
      WHERE Branch.branchNumber = e.branchNumber) = 'London'
      OR
      (SELECT branchName
      FROM Branch
      WHERE Branch.branchNumber = e.branchNumber) = 'Berlin'
  ORDER BY
      (
        SELECT man.salary
        FROM Employee AS man
        WHERE man.sin = (
          SELECT managerSIN
          FROM Branch
          WHERE e.branchNumber = Branch.branchNumber
        )
      ) - e.salary DESC
  LIMIT 10;
  `
}

function problem3() {
  return prisma.$queryRaw`
  SELECT 
    c.firstName, c.lastName, c.income 
  FROM Customer AS c
  WHERE income >= ALL (
    SELECT 2 * income
    FROM Customer
    WHERE lastName = "Butler"
  )
  ORDER BY lastName ASC, firstName ASC
  LIMIT 10;`
}

function problem4() {
  return prisma.$queryRaw`
  SELECT
    c.customerID,
    c.income,
    o.accNumber,
    a.branchNumber
  FROM
      Customer as c LEFT OUTER JOIN Owns as o
      ON c.customerID = o.customerID
      LEFT OUTER JOIN Account AS a
      ON o.accNumber = a.accNumber
  WHERE
      (c.income > 80000)
      AND
      EXISTS(
          SELECT c1.customerID
          FROM
              Customer as c1 LEFT OUTER JOIN Owns as o1
              ON c1.customerID = o1.customerID
              LEFT OUTER JOIN Account AS a1
              ON o1.accNumber = a1.accNumber
          WHERE a1.branchNumber = 1 AND
                c1.customerID = c.customerID
          )
      AND
      EXISTS(
          SELECT c1.customerID
          FROM
              Customer as c1 LEFT OUTER JOIN Owns as o1
              ON c1.customerID = o1.customerID
              LEFT OUTER JOIN Account AS a1
              ON o1.accNumber = a1.accNumber
          WHERE a1.branchNumber = 2 AND
                c1.customerID = c.customerID
      )

  ORDER BY c.customerID, o.accNumber
  LIMIT 10;
  `
}

function problem5() {
  return prisma.$queryRaw`
  SELECT
    c.customerID,
    a.type,
    o.accNumber,
    a.balance
  FROM
      Customer as c LEFT OUTER JOIN Owns as o
      ON c.customerID = o.customerID
      LEFT OUTER JOIN Account AS a
      ON o.accNumber = a.accNumber
  WHERE
      a.type = 'BUS'
      OR
      a.type = 'SAV'

  ORDER BY c.customerID, a.type, o.accNumber
  LIMIT 10;
  `
}

function problem6() {
  return prisma.$queryRaw`
  SELECT
    b.branchName,
    a.accNumber,
    a.balance
  FROM
      Branch AS b LEFT OUTER JOIN Account AS a
      ON b.branchNumber = a.branchNumber
  WHERE
      a.balance > 100000
      AND
      a.branchNumber = (
          SELECT b1.branchNumber
          FROM
              Employee as e1 INNER JOIN Branch as b1
              ON e1.sin = b1.managerSIN
          WHERE
              e1.firstName = 'Phillip'
              AND
              e1.lastName = 'Edwards'
          )

  ORDER BY a.accNumber
  LIMIT 10;
  `
}

function problem7() {
  return prisma.$queryRaw`
  SELECT
    DISTINCT c.customerID
  FROM
      Customer AS c LEFT OUTER JOIN Owns as o
      ON c.customerID = o.customerID
      LEFT OUTER JOIN Account AS a
      ON o.accNumber = a.accNumber
      LEFT OUTER JOIN Branch as b
      ON a.branchNumber = b.branchNumber
  WHERE
      EXISTS (
          SELECT
              c0.customerID
          FROM
              Customer AS c0 LEFT OUTER JOIN Owns as o0
              ON c0.customerID = o0.customerID
              LEFT OUTER JOIN Account AS a0
              ON o0.accNumber = a0.accNumber
              LEFT OUTER JOIN Branch as b0
              ON a0.branchNumber = b0.branchNumber
          WHERE c0.customerID = c.customerID AND
                b0.branchName = 'New York'
      )
      AND
      NOT EXISTS (
          SELECT
              c1.customerID
          FROM
              Customer AS c1 LEFT OUTER JOIN Owns as o1
              ON c1.customerID = o1.customerID
              LEFT OUTER JOIN Account AS a1
              ON o1.accNumber = a1.accNumber
              LEFT OUTER JOIN Branch as b1
              ON a1.branchNumber = b1.branchNumber
          WHERE c1.customerID = c.customerID AND
                b1.branchName = 'London'
      )
      AND
      c.customerID NOT IN (
          SELECT
              c4.customerID
          FROM
              Customer AS c4 LEFT OUTER JOIN Owns as o4
              ON c4.customerID = o4.customerID
              LEFT OUTER JOIN Account AS a4
              ON o4.accNumber = a4.accNumber
              LEFT OUTER JOIN Branch as b4
              ON a4.branchNumber = b4.branchNumber
          WHERE EXISTS(
              SELECT
                  c2.customerID
              FROM
                  Customer AS c2 LEFT OUTER JOIN Owns as o2
                  ON c2.customerID = o2.customerID
                  LEFT OUTER JOIN Account AS a2
                  ON o2.accNumber = a2.accNumber
                  LEFT OUTER JOIN Branch as b2
                  ON a2.branchNumber = b2.branchNumber
              WHERE
                  c2.customerID != c4.customerID AND
                  a2.accNumber = a4.accNumber AND
                  EXISTS (
                      SELECT c3.customerID
                      FROM
                          Customer AS c3 LEFT OUTER JOIN Owns as o3
                          ON c3.customerID = o3.customerID
                          LEFT OUTER JOIN Account AS a3
                          ON o3.accNumber = a3.accNumber
                          LEFT OUTER JOIN Branch as b3
                          ON a3.branchNumber = b3.branchNumber
                      WHERE c3.customerID = c2.customerID AND
                            b3.branchName = 'London'
                ))
      )

  ORDER BY c.customerID
  LIMIT 10;
  `
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