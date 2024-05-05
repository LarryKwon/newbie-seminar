import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()


function problem1() {
  return prisma.$queryRaw`SELECT firstName, lastName, income 
  FROM Customer WHERE 50000 <= income AND income <= 60000 
  ORDER BY income DESC, lastName ASC, firstName ASC 
  Limit 10;`
}

function problem2() {
  return prisma.$queryRaw`
  SELECT eempl.sin, Branch.branchName, eempl.salary, CAST((eman.salary-eempl.salary) AS CHAR(10)) AS 'Salary Diff'
  FROM Employee eempl
    JOIN Branch ON Branch.BranchNumber = eempl.BranchNumber
    JOIN Employee eman ON eman.sin = Branch.managerSIN
  WHERE Branch.branchName = "London" OR Branch.branchName = "Berlin"
  ORDER BY eman.salary-eempl.salary DESC
  LIMIT 10;`
}

function problem3() {
  return prisma.$queryRaw`
  SELECT firstName, lastName, income 
  FROM Customer
  WHERE income >= 2 * (SELECT MAX(income) FROM Customer WHERE lastName = "Butler")
  ORDER BY lastName ASC, firstName ASC
  LIMIT 10;`
}

function problem4() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.income, o.accNumber, a.branchNumber
  FROM Customer c
    JOIN Owns o ON o.CustomerID = c.CustomerID
    JOIN Account a ON a.accNumber = o.accNumber
  WHERE c.income > 80000
  AND (SELECT branchNumber FROM Branch WHERE branchName = 'London') IN (SELECT a1.branchNumber
        FROM Customer c1
          JOIN Owns o1 ON o1.CustomerID = c1.CustomerID
          JOIN Account a1 ON a1.accNumber = o1.accNumber
        WHERE c1.customerID = c.customerID)
  AND (SELECT branchNumber FROM Branch WHERE branchName = 'Latveria') IN (SELECT a2.branchNumber
      FROM Customer c2
        JOIN Owns o2 ON o2.CustomerID = c2.CustomerID
        JOIN Account a2 ON a2.accNumber = o2.accNumber
      WHERE c2.customerID = c.customerID)
  ORDER BY
    c.customerID ASC,
    a.accNumber ASC
  LIMIT 10;`
}

function problem5() {
  return prisma.$queryRaw`
  SELECT c.customerID, a.type, a.accNumber, a.balance 
  FROM Customer c
    JOIN Owns o ON o.customerID = c.customerID
    JOIN Account a ON a.accNumber = o.accNumber
  WHERE a.type = "BUS"
    OR a.type = "SAV"
  ORDER BY
	  c.customerID ASC,
    a.type ASC,
    a.accNumber ASC
LIMIT 10;`
}

function problem6() {
  return prisma.$queryRaw`
  SELECT b.branchName, a.accNumber, a.balance
  FROM Account a
	  JOIN Branch b ON b.branchNumber = a.branchNumber
  WHERE CAST(a.balance AS FLOAT) > 100000
  AND b.managerSIN IN (SELECT sin FROM Employee WHERE firstName = "Phillip" AND lastName = "Edwards")
  ORDER BY a.accNumber ASC
  LIMIT 10;`
}

function problem7() {
  return prisma.$queryRaw`
  SELECT c.customerID
  FROM Customer c
  WHERE c.customerID IN (SELECT c1.customerID
	  FROM Customer c1
		  JOIN Owns o1 ON o1.customerID = c1.customerID
		  JOIN Account a1 ON a1.accNumber = o1.accNumber
	  WHERE a1.branchNumber IN (SELECT b1.branchNumber FROM Branch b1 WHERE b1.branchName = "New York"))
  AND c.customerID NOT IN (SELECT c2.customerID
	  FROM Customer c2
		  JOIN Owns o2 ON o2.customerID = c2.customerID
		  JOIN Account a2 ON a2.accNumber = o2.accNumber
	  WHERE a2.branchNumber IN (SELECT b2.branchNumber FROM Branch b2 WHERE b2.branchName = "London"))
  AND c.customerID NOT IN (SELECT o2.customerID 
	  FROM Owns o2
	  WHERE o2.accNumber IN (SELECT o3.accNumber 
		  FROM Owns o3
		  WHERE o3.customerID IN (SELECT c4.customerID
			  FROM Customer c4
				  JOIN Owns o4 ON o4.customerID = c4.customerID
				  JOIN Account a4 ON a4.accNumber = o4.accNumber
			  WHERE a4.branchNumber IN (SELECT b4.branchNumber FROM Branch b4 WHERE b4.branchName = "London"))))
  ORDER BY
	  c.customerID ASC
  LIMIT 10;`
}

function problem8() {
  return prisma.$queryRaw`
  SELECT e.sin, e.firstName, e.lastName, e.salary, b.branchName
  FROM Employee e
	  LEFT OUTER JOIN Branch b
		  ON e.branchNumber = b.branchNumber AND e.sin = b.managerSIN
  WHERE
	  e.salary > 50000
  ORDER BY
	  b.branchName DESC,
      e.firstName ASC
  LIMIT 10;`
}

function problem9() {
  return prisma.$queryRaw`
  SELECT e.sin, e.firstName, e.lastName, e.salary, IF(e.sin = b.managerSIN, b.branchName, null) AS branchName
  FROM Employee e, Branch b
  WHERE e.salary > 50000
  AND e.branchNumber = b.branchNumber
  ORDER BY
	  branchName DESC,
    e.firstName ASC
  LIMIT 10;`
}

function problem10() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.firstName, c.lastName, c.income
  FROM Customer c
  WHERE c.income > 5000
  AND NOT EXISTS (
	  SELECT b1.branchNumber
    FROM Branch b1
      JOIN Account a1 ON a1.branchNumber = b1.branchNumber
      JOIN Owns o1 ON o1.accNumber = a1.accNumber
      JOIN Customer c1 ON c1.customerID = o1.customerID
	  WHERE c1.firstName = "Helen"
      AND c1.lastName = "Morgan"
      AND NOT EXISTS (
        SELECT a2.accNumber
        FROM Account a2
          JOIN Owns o2 ON o2.accNumber = a2.accNumber
        WHERE o2.customerID = c.customerID
          AND a2.branchNumber = b1.branchNumber)
	)
UNION
SELECT customerID, firstName, lastName, income
FROM Customer
WHERE firstName = "Helen" AND lastName = "Morgan" AND income > 5000
ORDER BY income DESC
LIMIT 10;`
}

function problem11() {
  return prisma.$queryRaw`
  SELECT e.sin, e.firstName, e.lastName, e.salary
  FROM Employee e
  WHERE e.salary IN (SELECT MIN(e1.salary) FROM Employee e1)
  AND e.branchNumber IN (SELECT branchNumber FROM Branch WHERE branchName = "Berlin")
  ORDER BY
    e.sin ASC
  LIMIT 10;`
}

function problem14() {
  return prisma.$queryRaw`
  SELECT CAST(SUM(e.salary) AS CHAR) AS "sum of employees salaries"
  FROM Employee e
  WHERE e.branchNumber IN (SELECT b.branchNumber FROM Branch b WHERE b.branchName = "Moscow");`
}

function problem15() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.firstName, c.lastName
  FROM Customer c
  WHERE (SELECT COUNT(*) FROM Branch b
	  WHERE b.branchNumber IN (SELECT a1.branchNumber 
      FROM Account a1
		    JOIN Owns o1 ON o1.accNumber = a1.accNumber
        JOIN Customer c1 ON c1.customerID = o1.customerID
	    WHERE c1.customerID = c.customerID)) = 4
  ORDER BY
	  c.lastName ASC,
    c.firstName ASC
  LIMIT 10;`
}


function problem17() {
  return prisma.$queryRaw`
  SELECT c.customerID, c.firstName, c.lastName, c.income, AVG(CAST(a.balance AS DOUBLE)) AS "average account balance" 
  FROM Customer c
	  JOIN Owns o ON o.customerID = c.customerID
    JOIN Account a ON a.accNumber = o.accNumber
  WHERE (SELECT COUNT(*) 
	  From Account a1
		  JOIN Owns o1 ON o1.accNumber = a1.accNumber
      JOIN Customer c1 ON c1.customerID = o1.customerID
	  WHERE c1.customerID = c.customerID) >= 3
	    AND SUBSTRING(c.lastName,1,1) = "S"
      AND INSTR(c.lastName, "e") > 0
  GROUP BY c.customerID
  ORDER BY
	  c.customerID ASC;`
}

function problem18() {
  return prisma.$queryRaw`
  SELECT a.accNumber, a.balance, SUM(CAST(Transactions.amount AS DOUBLE)) AS "sum of transaction amounts"
  FROM Account a
	  JOIN Transactions ON Transactions.accNumber = a.accNumber
  WHERE a.branchNumber IN (SELECT b1.branchNumber FROM Branch b1
	  WHERE b1.branchName = "Berlin")
	  AND (SELECT COUNT(*) FROM Transactions
		  WHERE Transactions.accNumber = a.accNumber) >= 10
  GROUP BY a.accNumber
  ORDER BY
	  SUM(CAST(Transactions.amount AS DOUBLE)) ASC
  LIMIT 10;`
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