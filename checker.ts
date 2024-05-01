import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`select firstName, lastName, income from Customer where income <= 60000 and income >= 50000 order by income desc, lastName asc, firstName asc LIMIT 10;`
}

function problem2() {
  return prisma.$queryRaw`SELECT e.sin, b.branchName, e.salary, cast((m.salary-e.salary) as char(10)) as "Salary Diff" from
  Employee e
      JOIN Branch b on e.branchNumber = b.branchNumber
      JOIN Employee m on b.managerSIN = m.sin
  where b.branchName ='London' or b.branchName='Berlin'
  order by m.salary-e.salary desc
  LIMIT 10;
   
  `
}

function problem3() {
  return prisma.$queryRaw`SELECT c.firstName,c.lastName,c.income from
  Customer c
      join (SELECT max(income) as c2 from Customer where lastName = 'Butler') c2
  where c.income>=2*c2
  order by c.lastName asc, c.firstName asc
  LIMIT 10;
  `
}

function problem4() {
  return prisma.$queryRaw`SELECT c.customerID,c.income,a.accNumber,a.branchNumber from
  Owns o
      join Customer c on o.customerID = c.customerID
      join Account a on o.accNumber = a.accNumber
  where c.income>=80000
  and exists(
      SELECT 1
      from Owns o2
          join Account a2 on o2.accNumber = a2.accNumber
          join Branch b2 on a2.branchNumber = b2.branchNumber
      where o2.customerID = o.customerID
      and 'London' = b2.branchName
  )
  and exists(
      SELECT 1
      from Owns o2
          join Account a2 on o2.accNumber = a2.accNumber
          join Branch b2 on a2.branchNumber = b2.branchNumber
      where o2.customerID = o.customerID
      and 'Latveria' = b2.branchName
  )
  order by c.customerID asc, a.accNumber asc
  LIMIT 10;
  `
}

function problem5() {
  return prisma.$queryRaw`select * from Customer`
}

function problem6() {
  return prisma.$queryRaw`select * from Customer`
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