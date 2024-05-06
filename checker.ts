import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()

/**
 * Join을 약간 혼용해서 쓰시는 거 같은데,
 * 기본적으로 Left Join
 * Inner Join
 * Cartesian Join이 있습니다.
 * 이 중 맨 마지막 Cartesian Join이 일반적으로 지금 사용하시는 Join이고요,
 * Left Join이 Left outer join으로 쓰신 겁니다.
 * 각각 1:N, N:1 상황에서 어떤 Join을 쓰느냐에 따라 연산량이 달라지기 때문에 이 부분 고려하셔서 찾아보시면 될 것 같습니다.
 */


function problem1() {
  return prisma.$queryRaw`select firstName, lastName, income from Customer where income <= 60000 and income >= 50000 order by income desc, lastName asc, firstName asc LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw
  `
  SELECT e.sin, b.branchName, e.salary, CAST((e2.salary - e.salary) AS CHAR) AS 'Salary Diff'
  FROM Employee e
          JOIN Branch b ON e.branchNumber = b.branchNumber
          JOIN Employee e2 ON e2.sin = b.managerSIN
  WHERE b.branchName IN ('London', 'Berlin')
  ORDER BY e2.salary - e.salary DESC
  LIMIT 10;
  `
}

function problem3() {
  return prisma.$queryRaw`
  SELECT firstName, lastName, income
  FROM Customer
  WHERE income >= ALL (SELECT 2 * income FROM Customer WHERE lastName = 'Butler')
  ORDER BY lastName ASC, firstName ASC
  LIMIT 10;
  `
}

function problem4() {
  return prisma.$queryRaw`
  SELECT DISTINCT O.customerID, C.income, O.accNumber, A.branchNumber
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
  return prisma.$queryRaw`
SELECT DISTINCT c.customerID
FROM Customer AS c
         LEFT OUTER JOIN Owns AS o ON c.customerID = o.customerID
         LEFT OUTER JOIN Account AS a ON o.accNumber = a.accNumber
         LEFT OUTER JOIN Branch AS b ON a.branchNumber = b.branchNumber
WHERE c.customerID NOT IN (
    SELECT c4.customerID
    FROM Customer AS c4
             LEFT OUTER JOIN Owns AS o4 ON c4.customerID = o4.customerID
             LEFT OUTER JOIN Account AS a4 ON o4.accNumber = a4.accNumber
             LEFT OUTER JOIN Branch AS b4 ON a4.branchNumber = b4.branchNumber
    WHERE EXISTS (
        SELECT c2.customerID
        FROM Customer AS c2
                 LEFT OUTER JOIN Owns AS o2 ON c2.customerID = o2.customerID
                 LEFT OUTER JOIN Account AS a2 ON o2.accNumber = a2.accNumber
                 LEFT OUTER JOIN Branch AS b2 ON a2.branchNumber = b2.branchNumber
        WHERE c2.customerID != c4.customerID
          AND a2.accNumber = a4.accNumber
          AND EXISTS (
            SELECT c3.customerID
            FROM Customer AS c3
                     LEFT OUTER JOIN Owns AS o3 ON c3.customerID = o3.customerID
                     LEFT OUTER JOIN Account AS a3 ON o3.accNumber = a3.accNumber
                     LEFT OUTER JOIN Branch AS b3 ON a3.branchNumber = b3.branchNumber
            WHERE c3.customerID = c2.customerID
              AND b3.branchName = 'London'
        )
    )
)
  AND EXISTS (
    SELECT c0.customerID
    FROM Customer AS c0
             LEFT OUTER JOIN Owns AS o0 ON c0.customerID = o0.customerID
             LEFT OUTER JOIN Account AS a0 ON o0.accNumber = a0.accNumber
             LEFT OUTER JOIN Branch AS b0 ON a0.branchNumber = b0.branchNumber
    WHERE c0.customerID = c.customerID
      AND b0.branchName = 'New York'
)
  AND NOT EXISTS (
    SELECT c1.customerID
    FROM Customer AS c1
             LEFT OUTER JOIN Owns AS o1 ON c1.customerID = o1.customerID
             LEFT OUTER JOIN Account AS a1 ON o1.accNumber = a1.accNumber
             LEFT OUTER JOIN Branch AS b1 ON a1.branchNumber = b1.branchNumber
    WHERE c1.customerID = c.customerID
      AND b1.branchName = 'London'
)
ORDER BY c.customerID
LIMIT 10;
  `
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
  return prisma.$queryRaw`
SELECT
  c.customerID,
  c.firstName,
  c.lastName,
  c.income
FROM
    Customer AS c
        JOIN Owns AS o ON c.customerID = o.customerID
        JOIN Account AS a ON o.accNumber = a.accNumber
        JOIN (
        SELECT DISTINCT branchNumber
        FROM Account
        WHERE accNumber IN (
            SELECT accNumber FROM Owns WHERE customerID = (
                SELECT customerID FROM Customer
                WHERE firstName = 'Helen' AND lastName = 'Morgan'
            )
        )
    ) AS HelenBranches ON a.branchNumber = HelenBranches.branchNumber
WHERE
    c.income > 5000
GROUP BY
    c.customerID, c.firstName, c.lastName, c.income
HAVING
    COUNT(DISTINCT a.branchNumber) = (
        SELECT COUNT(DISTINCT branchNumber)
        FROM Account
        WHERE accNumber IN (
            SELECT accNumber FROM Owns WHERE customerID = (
                SELECT customerID FROM Customer
                WHERE firstName = 'Helen' AND lastName = 'Morgan'
            )
        )
    )
ORDER BY
    c.income DESC
LIMIT 10;
`
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
  SELECT CAST( SUM(salary) AS CHAR ) AS \`sum of employees salaries\`
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
  return prisma.$queryRaw`
SELECT a.accNumber AS "accNumber", a.balance AS "balance", SUM(t.amount) AS "sum of transaction amounts"
FROM Customer AS c
         INNER JOIN Owns AS o ON c.customerID = o.customerID
         INNER JOIN Account AS a ON o.accNumber = a.accNumber
         INNER JOIN Branch AS b ON a.branchNumber = b.branchNumber
         INNER JOIN Transactions AS t ON a.accNumber = t.accNumber
WHERE b.branchName = 'Berlin'
GROUP BY a.accNumber, a.balance HAVING COUNT(DISTINCT t.transNumber) >= 10
ORDER BY SUM(t.amount)
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