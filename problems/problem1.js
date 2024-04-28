const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

async function problem1() {
  try {
    const customers = await prisma.customer.findMany({
      select: {
        firstName: true,
        lastName: true,
        income: true
      },
      where: {
        income: {
          gte: 50000,
          lte: 60000
        }
      },
      orderBy: [
        { income: 'desc' },
        { lastName: 'asc' },
        { firstName: 'asc' }
      ],
      take: 10
    });

    return customers;
  } catch (error) {
    console.error('Error executing Prisma query:', error);
    return null;
  }
}


async function main() {
  try {
    const result = await problem1();
    console.log(result);
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await prisma.$disconnect();
  }
}

main();
