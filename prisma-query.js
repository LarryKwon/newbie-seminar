const { PrismaClient } = require('@prisma/client');
const express = require('express');
const router = express.Router();
const prisma = new PrismaClient();

async function problem1() {
  return await prisma.customer.findMany({
    where: {
      income: {
        gte: 50000,
        lte: 60000
      }
    },
    select: {
      firstName: true,
      lastName: true,
      income: true
    },
    orderBy: [
      { income: 'desc' },
      { lastName: 'asc' },
      { firstName: 'asc' }
    ],
    take: 10
  });
}

const ProblemList = [
  { id: 1, query: problem1 }, { id: 2, query: problem1 }, { id: 3, query: problem1 }, { id: 4, query: problem1 }, { id: 5, query: problem1 }, 
  { id: 6, query: problem1 }, { id: 7, query: problem1 }, { id: 8, query: problem1 }, { id: 9, query: problem1 }, { id: 10, query: problem1 }
];

async function setupEndpoints() {
  for (const problem of ProblemList) {
    const result = await problem.query();
    if (result && result.length > 0) {
      router.get(`/${problem.id}`, (req, res) => {
        res.json(result);
      });
    }
  }
}

setupEndpoints().then(() => {
  console.log('Problem endpoints set up.');
});

module.exports = router;