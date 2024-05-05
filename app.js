const express = require("express");
const { PrismaClient } = require("@prisma/client");

const app = express();
app.use(express.json());
const prisma = new PrismaClient();
const PORT = 3000;

app.get("/", (req, res) => {
  res.send("get");
});

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

app.get("/problems/1", async (req, res) => {
  const result = await prisma.customer.findMany({
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
  res.json(result);
});