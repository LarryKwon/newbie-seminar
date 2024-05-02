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
  return prisma.$queryRaw`SELECT c.customerID,a.type,a.accNumber,a.balance from
  Account a
      join Owns o on a.accNumber = o.accNumber
      join Customer c on o.customerID = c.customerID
  where a.type in ('SAV', 'BUS')
  order by c.customerID asc, a.type asc, a.accNumber asc
  LIMIT 10;
  `
}

function problem6() {
  return prisma.$queryRaw`SELECT b.branchName, a.accNumber, a.balance from
  Account a
      join Branch b on a.branchNumber = b.branchNumber
      join Employee e on b.managerSIN = e.sin
  where a.balance>100000 and e.firstName='Phillip' and e.lastName='Edwards'
  order by a.accNumber
  LIMIT 10;
  `
}

function problem7() {
  return prisma.$queryRaw`SELECT c.customerID from
  Customer c
  where EXISTS(
      SELECT 1 from
      Owns o
          join Account a on o.accNumber = a.accNumber
          join Branch b on a.branchNumber = b.branchNumber
      where c.customerID=o.customerID and
            b.branchName='New York'
  ) and not EXISTS( # London인 친구 있는지 반환:자기 자신 포함
      SELECT 1 from
      Owns o
      where c.customerID=o.customerID and
      EXISTS(SELECT 1
             from Owns o3
             where o3.accNumber = o.accNumber
               and EXISTS(
                 SELECT 1 from
                 Customer c2
                 where c2.customerID = o3.customerID and
                       EXISTS(SELECT 1
                              from Owns o2
                                       join Account a2 on o2.accNumber = a2.accNumber
                                       join Branch b2 on a2.branchNumber = b2.branchNumber
                              where o2.customerID = c2.customerID
                                and b2.branchName = 'London')))
  )
  order by c.customerId
  LIMIT 10;  
  `
}

function problem8() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary, b.branchName from
  Employee e
      left join Branch b on e.sin = b.managerSIN
  where e.salary>50000
  order by b.branchName desc,binary e.firstName
  LIMIT 10;  
  `
}

function problem9() {
  return prisma.$queryRaw`SELECT e.sin, e.firstName, e.lastName, e.salary, b.branchName from
  Employee e,Branch b
  where e.salary>50000 and
  e.sin = b.managerSIN
  union
  SELECT e.sin, e.firstName, e.lastName, e.salary, null as branchName from
  Employee e
  where e.salary>50000 and
  not EXISTS(
      SELECT 1 from
      Branch b
      where e.sin = b.managerSIN
  )
  order by branchName desc,binary firstName
  LIMIT 10;
  `
}

function problem10() {
  return prisma.$queryRaw`
  SELECT customerID, firstName, lastName, income from
  Customer c
  where income>5000 and
  not EXISTS(
      SELECT 1 from # helen 뭐시기가 not EXISTS인가
      Owns o
          join Customer c2 on o.customerID = c2.customerID
          join Account a on o.accNumber = a.accNumber
          join Branch b on a.branchNumber = b.branchNumber
      where c2.firstName='Helen' and c2.lastName='Morgan' and
      not EXISTS(
          SELECT 1 from
          Owns o2
              join Account a2 on o2.accNumber = a2.accNumber
              join Branch b2 on a2.branchNumber = b2.branchNumber
          where o2.customerID=c.customerID and b.branchName=b2.branchName
      )
  )
  order by income desc
  LIMIT 10;
  `
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