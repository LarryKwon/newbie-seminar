import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'


const prisma = new PrismaClient()




function problem1() {
  return prisma.$queryRaw`select firstName, lastName, income from Customer where income <= 60000 and income >= 50000 order by income desc, lastName asc, firstName asc LIMIT 10;`}

function problem2() {
  return prisma.$queryRaw`select e.sin, b.branchName, e.salary, CAST((m.salary - e.salary) AS CHAR(10))  as "Salary Diff"
  from Employee e
  join Branch b on e.branchNumber = b.branchNumber
  join Employee m on b.managerSIN = m.sin
  where b.branchName in ('London', 'Berlin')
  order by m.salary-e.salary desc
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
  return prisma.$queryRaw`select c.customerID, c.income, a.accNumber, a.branchNumber 
  from Customer c
  join Owns o on o.customerID = c.customerID
  join Account a on o.accNumber = a.accNumber
  where c.income>80000 
  and c.customerID IN (
    select o.customerID
    from Owns o
    join Account a on o.accNumber = a.accNumber
    where a.branchNumber in (
        select b.branchNumber
        from Branch b
        where b.branchName = 'London' or b.branchName = 'Latveria'
    )
    group by o.customerID
    having COUNT(DISTINCT a.branchNumber) = 2
)
  order by c.customerID asc, a.accNumber asc
  LIMIT 10;`
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
  return prisma.$queryRaw`select c.customerID
  from Customer c
  join Owns o on o.customerID = c.customerID
  join Account a on o.accNumber = a.accNumber
  where a.branchNumber = (select branchNumber from Branch where branchName = 'New York')
  and c.customerID not in (
    select o.customerID
    from Owns o
    join Account a on o.accNumber = a.accNumber
    where a.branchNumber = (select branchNumber from Branch where branchName = 'London')
  )
  and c.customerID not in (
    select DISTINCT o1.customerID
    from Owns o1
    join Owns o2 on o1.accNumber = o2.accNumber and o1.customerID != o2.customerID
    where o2.customerID in (
    select o.customerID
    from Owns o
    join Account a on o.accNumber = a.accNumber
    where a.branchNumber = (select branchNumber from Branch where branchName = 'London')
  )
  )
  order by c.customerID asc
  LIMIT 10`
}

function problem8() {
  return prisma.$queryRaw`select e.sin, e.firstName, e.lastName, e.salary,
  case when e.sin = b.managerSIN then b.branchName else null end as branchName
  from Employee e
  left join Branch b on e.sin = b.managerSIN
  where e.salary > 50000
  order by branchName desc, e.firstName asc
  LIMIT 10`
}

function problem9() {
  return prisma.$queryRaw`select * from Customer`
}

function problem10() {
  return prisma.$queryRaw`select * from Customer`
}

function problem11() {
  return prisma.$queryRaw`select e.sin, e.firstName, e.lastName, e.salary
  from Employee e
  join Branch b on e.branchNumber = b.branchNumber
  where b.branchName = 'Berlin' and e.salary = (
    select min(e2.salary)
    from Employee e2
    join Branch b2 on e2.branchNumber = b2.branchNumber
    where b2.branchName = 'Berlin'
  )
  order by e.sin asc
  LIMIT 10;`
}

function problem14() {
  return prisma.$queryRaw`select CAST(sum(e.salary) AS CHAR(10))as "sum of employees salaries"
  from Employee e
  join Branch b on b.branchNumber = e.branchNumber
  where b.branchName = 'Moscow'
  LIMIT 10;`
}

function problem15() {
  return prisma.$queryRaw`select c.customerID, c.firstName, c.lastName
  from Customer c
  join Owns o on o.customerID = c.customerID
  join Account a on o.accNumber = a.accNumber
  join Branch b on b.branchNumber = a.branchNumber
  group by c.customerID, c.firstName, c.lastName
  having count(DISTINCT b.branchName) = 4
  order by c.lastName asc, c.firstName asc
  LIMIT 10;`
}


function problem17() {
  return prisma.$queryRaw`select c.customerID, c.firstName, c.lastName, c.income,
  avg(a.balance) as "average account balance"
  from Customer c
  join Owns o on o.customerID = c.customerID
  join Account a on o.accNumber = a.accNumber
  where c.lastName like 'S%e%'
  group by c.customerID, c.firstName, c.lastName, c.income
  having count(DISTINCT a.accNumber) >= 3
  order by c.customerID asc
  LIMIT 10;`
}

function problem18() {
  return prisma.$queryRaw`select a.accNumber, a.balance, sum(t.amount) as "sum of transaction amounts"
  from Account a
  join Transactions t on a.accNumber = t.accNumber
  join Branch b on b.branchNumber = a.branchNumber
  where b.branchName = 'Berlin'
  group by a.accNumber, a.balance
  having count(t.transNumber) >= 10
  order by sum(t.amount) asc
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