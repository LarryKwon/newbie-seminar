import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`select firstName, lastName, income from Customer where income <= 60000 and income >= 50000 order by income desc, lastName asc, firstName asc LIMIT 10;`}

function problem2() {
  return prisma.$queryRaw`select e.sin, b.branchName, e.salary, m.salary-e.salary as \`Salary Diff\`
  from Employee e
  join Branch b on e.branchNumber = b.branchNumber
  join Employee m on b.managerSIN = m.sin
  where b.branchName in ('London', 'Berlin')
  order by \`Salary Diff\` desc
  LIMIT 10;`
}

function problem3() {
  return prisma.$queryRaw`select c.firstName, c.lastName, c.income from Customer c
  where c.income >= all (
    select 2*c2.income
    from Customer c2
    where c2.lastName = 'Butler'
  )
  order by lastName asc, firstName asc 
  LIMIT 10;`
}

function problem4() {
  return prisma.$queryRaw`select * from Customer`
}

function problem5() {
  return prisma.$queryRaw`select c.customerID, a.type, a.accNumber, a.balance 
  from Customer c
  join Owns o on o.customerID = c.customerID
  join Account a on o.accNumber = a.accNumber
  where a.type in ('BUS', 'SAV')
  order by c.customerID asc, a.type asc, a.accNumber asc
  LIMIT 10;`
}

function problem6() {
  return prisma.$queryRaw`select b.branchName, a.accNumber, a.balance 
  from Branch b 
  join Account a on b.branchNumber = a.branchNumber
  where a.balance > 100000
  order by a.accNumber asc
  LIMIT 10;`  
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
    if (i==5){console.log(answer); console.log(result)};
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