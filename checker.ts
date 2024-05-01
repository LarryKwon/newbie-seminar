import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'

const prisma = new PrismaClient()

/*
 * About bank.txt
 * bank.txt는 sql dump 파일로, bank 테이블의 구조와 데이터가 들어있습니다.
 * 해당 파일을 sql 명령어로서 실행시킬 경우, bank라는 이름의 database와
 * 포함된 table들, data들이 생성됩니다.
 * bank database는 가상 은행의 고객 정보, 계좌 정보와 잔액 정보 등을 담고 있습니다.
 * Account table에는 모든 계좌의 id, 형태, 잔액, 관리 지점이 포함되어 있으며,
 * Branch table에는 은행 지점 정보, Customer tavle에는 고객 정보가 포함되어 있습니다.
 * 또한 Employee table에는 직원 정보가, Owns 테이블에는 Account와 Customer 사이의 relation
 * 정보를 담고 있습니다. Transactions table에는 거래 기록이 있습니다.
*/

function problem1() {
  return prisma.$queryRaw`SELECT firstName, lastName, income FROM Customer WHERE income BETWEEN 50000 AND 60000 ORDER BY income DESC, lastName, firstName LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw`SELECT E.sin, B.branchName, E.salary, CAST(M.salary - E.salary AS CHAR(5)) AS 'Salary Diff' FROM Employee E, Employee M, Branch B WHERE E.branchNumber = B.branchNumber AND B.managerSIN = M.sin AND (B.branchName = 'London' OR B.branchName = 'Berlin') ORDER BY (M.salary - E.salary) DESC LIMIT 10;`
}

function problem3() {
  return prisma.$queryRaw`SELECT firstName, lastName, income FROM Customer WHERE income >= ALL(SELECT income * 2 FROM Customer WHERE lastName = 'Butler') ORDER BY lastName, firstName LIMIT 10;`
}

function problem4() {
  return prisma.$queryRaw`SELECT Customer.customerID, income, Owns.accNumber, Account.branchNumber FROM Customer, Owns, Account, Branch WHERE income > 80000 AND Customer.customerID = ANY(SELECT customerID FROM Owns, Account, Branch WHERE branchName = 'London' AND Branch.branchNumber = Account.branchNumber AND Owns.accNumber = Account.accNumber) AND Customer.customerID = ANY(SELECT customerID FROM Owns, Account, Branch WHERE branchName = 'Latveria' AND Branch.branchNumber = Account.branchNumber AND Owns.accNumber = Account.accNumber) AND Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber AND Account.branchNumber = Branch.branchNumber ORDER BY Customer.customerID, Account.accNumber LIMIT 10;`
}

function problem5() {
  return prisma.$queryRaw`SELECT Customer.customerID, type, Account.accNumber, balance FROM Customer, Owns, Account WHERE Customer.customerID = ANY(SELECT customerID FROM Owns, Account WHERE type IN ('BUS', 'SAV')) AND type IN ('BUS', 'SAV') AND Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber ORDER BY customerID, type, accNumber LIMIT 10;`
}

function problem6() {
  return prisma.$queryRaw`SELECT Branch.branchName, Account.accNumber, Account.balance FROM Branch, Account, Employee WHERE balance > 100000 AND Employee.firstName = 'Phillip' AND Employee.lastName = 'Edwards' AND Employee.branchNumber = Branch.branchNumber AND Account.branchNumber = Branch.branchNumber ORDER BY accNumber LIMIT 10;`
}

function problem7() {
  return prisma.$queryRaw`SELECT Customer.customerID FROM Customer, Owns, Account WHERE Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber
  AND Customer.customerID = ANY(SELECT Customer.customerID FROM Customer, Owns, Account, Branch WHERE Customer.customerID = Owns.customerID AND Account.accNumber = Owns.accNumber AND Account.branchNumber = Branch.branchNumber AND branchName = 'New York')
  AND Customer.customerID != ALL(SELECT Customer.customerID FROM Customer, Owns, Account WHERE Customer.customerID = Owns.customerID AND Account.accNumber = Owns.accNumber
    AND Account.accNumber = ANY(SELECT Account.accNumber FROM Customer, Owns, Account WHERE Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber
      AND Customer.customerID = ANY(SELECT Customer.customerID FROM Customer, Owns, Account, Branch WHERE Customer.customerID = Owns.customerID AND Account.accNumber = Owns.accNumber AND Account.branchNumber = Branch.branchNumber AND branchName = 'London')))
  GROUP BY Customer.customerID ORDER BY customerID LIMIT 10;`
}

function problem8() {
  return prisma.$queryRaw`SELECT Employee.sin, Employee.firstName, Employee.lastName, Employee.salary, Branch.branchName FROM Employee LEFT JOIN Branch ON Employee.sin = Branch.managerSIN WHERE salary > 50000 ORDER BY branchName DESC, firstName LIMIT 10;`
}

function problem9() {
  return prisma.$queryRaw`SELECT Employee.sin, Employee.firstName, Employee.lastName, Employee.salary, CASE WHEN sin = managerSIN THEN branchName END AS 'branchName' FROM Employee, Branch WHERE salary > 50000 AND Employee.branchNumber = Branch.branchNumber ORDER BY branchName DESC, firstName LIMIT 10;`
}

function problem10() {
  return prisma.$queryRaw`SELECT Customer.customerID, Customer.firstName, Customer.lastName, Customer.income FROM Customer, Owns, Account WHERE
  Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber AND income > 5000 AND
  branchNumber = ANY(SELECT branchNumber FROM Customer, Owns, Account WHERE Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber AND firstName = 'Helen' AND lastName = 'Morgan')
  GROUP BY Customer.customerID HAVING COUNT(DISTINCT branchNumber) = (SELECT COUNT(branchNumber) FROM Customer, Owns, Account WHERE Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber AND firstName = 'Helen' AND lastName = 'Morgan')
  ORDER BY income DESC LIMIT 10;`
}

function problem11() {
  return prisma.$queryRaw`SELECT Employee.sin, Employee.firstName, Employee.lastName, Employee.salary FROM Employee WHERE Employee.salary = (SELECT MIN(salary) FROM Employee) ORDER BY sin LIMIT 10;`
}

function problem14() {
  return prisma.$queryRaw`SELECT CAST(SUM(Employee.salary) AS CHAR(6)) AS 'sum of employees salaries' FROM Employee, Branch WHERE Employee.branchNumber = Branch.branchNumber AND branchName = 'Moscow';`
}

function problem15() {
  return prisma.$queryRaw`SELECT Customer.customerID, Customer.firstName, Customer.lastName FROM Customer, Owns, Account, Branch WHERE
  Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber AND Account.branchNumber = Branch.branchNumber
  GROUP BY Customer.customerID HAVING COUNT(DISTINCT Branch.branchNumber) = 4 ORDER BY MIN(Customer.lastName), MIN(Customer.firstName) LIMIT 10;`
}

function problem17() {
  return prisma.$queryRaw`SELECT Customer.customerID, Customer.firstName, Customer.lastName, Customer.income, AVG(Account.balance) 'average account balance' FROM Customer, Owns, Account
  WHERE Customer.customerID = Owns.customerID AND Owns.accNumber = Account.accNumber AND lastName REGEXP 'S.*e.*'
  GROUP BY Customer.customerID HAVING COUNT(Owns.accNumber) >= 3 ORDER BY customerID LIMIT 10;`
}

function problem18() {
  return prisma.$queryRaw`SELECT Account.accNumber, Account.balance, sum(Transactions.amount) 'sum of transaction amounts' FROM Account, Transactions, Branch
  WHERE Transactions.accNumber = Account.accNumber AND Branch.branchNumber = Account.branchNumber AND branchName = 'Berlin'
  GROUP BY Account.accNumber HAVING COUNT(transNumber) >= 10 ORDER BY sum(Transactions.amount) LIMIT 10;`
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
