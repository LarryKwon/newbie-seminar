import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`SELECT firstName, lastName, income FROM Customer WHERE income <= 60000 and income >= 50000 ORDER BY income DESC, lastName ASC, firstName ASC LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw`SELECT e.sin, b.branchName, e.salary, CAST((m.salary - e.salary) AS char(10)) AS 'Salary Diff'
FROM Employee e
JOIN Branch b ON e.branchNumber=b.branchNumber
JOIN Employee m ON b.managerSIN=m.SIN
WHERE b.branchName='London' or b.branchName='Berlin'
ORDER BY (m.salary - e.salary) DESC
LIMIT 10;
 `;
}

function problem3() {
  return prisma.$queryRaw`SELECT c.firstName, c.lastName, c.income
FROM Customer c
WHERE income >= all (
    SELECT 2*butler.income
    FROM Customer butler
    WHERE butler.lastName='Butler'
)
ORDER BY lastName ASC, firstName ASC
LIMIT 10;`
}

function problem4() {
  return prisma.$queryRaw`SELECT c.customerID, c.income, a.accNumber, a.branchNumber
FROM Owns o
JOIN Customer c on c.customerID=o.customerID
JOIN Account a on o.accNumber=a.accNumber
WHERE c.income>=80000 and exists(
    SELECT 1
    FROM Owns o2
    JOIN Account a2 on o2.accNumber=a2.accNumber
    JOIN Branch b2 on a2.branchNumber=b2.branchNumber
    WHERE o2.customerID=o.customerID and b2.branchName='London'
) and exists(
    SELECT 1
    FROM Owns o2
    JOIN Account a2 on o2.accNumber=a2.accNumber
    JOIN Branch b2 on a2.branchNumber=b2.branchNumber
    WHERE o2.customerID=o.customerID and b2.branchName='Latveria'
)
ORDER BY c.customerID ASC, a.accNumber ASC
LIMIT 10;`
}

function problem5() {
  return prisma.$queryRaw`SELECT c.customerID, a.type, a.accNumber, a.balance
FROM Customer c
JOIN Owns o on c.customerID=o.customerID
JOIN Account a on o.accNumber=a.accNumber
WHERE a.type='BUS' or a.type='SAV'
ORDER BY c.customerID ASC, a.type ASC, a.accNumber ASC
LIMIT 10;`
}

function problem6() {
  return prisma.$queryRaw`SELECT b.branchName, a.accNumber, a.balance
FROM Branch b
JOIN Employee e on b.managerSIN=e.sin
JOIN Account a on b.branchNumber=a.branchNumber
WHERE a.balance>100000 and e.firstName='Phillip' and e.lastName='Edwards'
ORDER BY a.accNumber ASC
LIMIT 10;`
}

function problem7() {
  return prisma.$queryRaw`SELECT c.customerID
FROM Customer c
JOIN Owns o on o.customerID = c.customerID
JOIN Account a on o.accNumber = a.accNumber
WHERE a.branchNumber = (SELECT branchNumber FROM Branch WHERE branchName='New York')
and c.customerID NOT IN (
    SELECT o.customerID
    FROM Owns o
    JOIN Account a on o.accNumber = a.accNumber
    WHERE a.branchNumber = (SELECT branchNumber FROM Branch WHERE branchName='London')
)
and c.customerID NOT IN (
    SELECT o1.customerID
    FROM Owns o1
    JOIN Owns o2 on o1.accNumber=o2.accNumber and o1.customerID!=o2.customerID
    WHERE o2.customerID IN (
        SELECT o.customerID
        FROM Owns o
        JOIN Account a on o.accNumber = a.accNumber
        WHERE a.branchNumber = (SELECT branchNumber FROM Branch WHERE branchName='London')
    )
)
ORDER BY c.customerID ASC
LIMIT 10;`
}

function problem8() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary, b.branchName
FROM Employee e
LEFT OUTER JOIN Branch b on e.sin=b.managerSIN
WHERE e.salary>50000
ORDER BY b.branchName DESC, e.firstName ASC
LIMIT 10;`
}

function problem9() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary,
       CASE WHEN exists(SELECT 1 FROM Employee e, Branch b WHERE b.managerSIN=e.sin)
           THEN (SELECT b.branchName FROM Branch b WHERE b.managerSIN=e.sin)
           END as branchName
FROM Employee e
WHERE e.salary>50000
ORDER BY branchName DESC, e.firstName ASC
LIMIT 10;`
}

function problem10() {
  return prisma.$queryRaw`SELECT DISTINCT c.customerID, c.firstName, c.lastName, c.income
FROM Customer c
JOIN Owns o on c.customerID=o.customerID
JOIN Account a on o.accNumber=a.accNumber
WHERE c.income>5000 and NOT exists(
    SELECT 1
    FROM (
        SELECT b.branchNumber
        FROM Customer c2
        JOIN Owns o2 on c2.customerID=o2.customerID
        JOIN Account a2 on o2.accNumber=a2.accNumber
        JOIN Branch b on a2.branchNumber=b.branchNumber
        WHERE c2.firstName='Helen' and c2.lastName='Morgan'
    ) selected_branch
    WHERE selected_branch.branchNumber NOT IN (
        SELECT a3.branchNumber
        FROM Owns o3
        JOIN Account a3 on o3.accNumber=a3.accNumber
        WHERE o3.customerID=c.customerID
    )
)
ORDER BY c.income DESC
LIMIT 10;`
}

function problem11() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary
FROM Employee e
JOIN Branch b on e.branchNumber=b.branchNumber
WHERE b.branchName='Berlin' and e.salary=(SELECT min(e.salary) FROM Employee e JOIN Branch b on e.branchNumber=b.branchNumber WHERE b.branchName='Berlin')
ORDER BY e.sin ASC
LIMIT 10;`
}

function problem14() {
  return prisma.$queryRaw`SELECT CAST(sum(e.salary) as char(10)) as 'sum of employees salaries'
FROM Employee e
JOIN Branch b on e.branchNumber=b.branchNumber
WHERE b.branchName='Moscow'
LIMIT 10;`
}

function problem15() {
  return prisma.$queryRaw`SELECT c.customerID, c.firstName, c.lastName
FROM Customer c
JOIN Owns o on c.customerID=o.customerID
JOIN Account a on o.accNumber=a.accNumber
JOIN Branch b on a.branchNumber=b.branchNumber
GROUP BY c.customerID, c.firstName, c.lastName
HAVING count(DISTINCT b.branchNumber)=4
ORDER BY lastName ASC, firstName ASC
LIMIT 10;`
}


function problem17() {
  return prisma.$queryRaw`SELECT c.customerID, c.firstName, c.lastName, c.income, avg(a.balance) as "average account balance"
FROM Customer c
JOIN Owns o on c.customerID=o.customerID
JOIN Account a on o.accNumber=a.accNumber
WHERE left(c.lastName, 1)='s' and instr(c.lastName, 'e')
GROUP BY c.customerID, c.firstName, c.lastName, c.income
HAVING count(DISTINCT a.accNumber)>=3
ORDER BY c.customerID ASC
LIMIT 10;`
}

function problem18() {
  return prisma.$queryRaw`SELECT a.accNumber, a.balance, sum(t.amount) as 'sum of transaction amounts'
FROM Account a
JOIN Transactions t on a.accNumber=t.accNumber
JOIN Branch b on a.branchNumber=b.branchNumber
WHERE b.branchName='Berlin'
GROUP BY a.accNumber, a.balance
HAVING count(t.transNumber)>=10
ORDER BY sum(t.amount) ASC
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