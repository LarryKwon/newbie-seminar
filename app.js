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

app.get("/problems/2", async (req, res) => {
  const employees = await prisma.employee.findMany({
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: {
          in: ["London", "Berlin"],
        },
      },
    },
    include: {
      Branch_Employee_branchNumberToBranch: {
        select: {
          branchName: true,
          Employee_Branch_managerSINToEmployee: {
            select: {
              salary: true
            }
          }
        },
      },
    },
  });
  const result = employees.map((e) => ({
      sin: e.sin,
      branchName: e.Branch_Employee_branchNumberToBranch.branchName,
      salary: e.salary,
      "Salary Diff": (e.Branch_Employee_branchNumberToBranch.Employee_Branch_managerSINToEmployee.salary - e.salary).toString()
    })).sort((a, b) => b["Salary Diff"] - a["Salary Diff"])
    .slice(0, 10);
  res.json(result);
});

app.get("/problems/3", async (req, res) => {
  const select_butler = await prisma.customer.findMany({
    where: {
      lastName: "Butler",
    },
  });

  const maxIncome = select_butler.reduce((max, customer) => {
    return Math.max(max, customer.income);
  }, -Infinity);
  

  const doubleMaxIncome = maxIncome * 2;

  const result = await prisma.customer.findMany({
    where: {
      income: {
        gte: doubleMaxIncome,
      },
    },
    orderBy: [{ lastName: "asc" }, { firstName: "asc" }],
    take: 10,
    select: {
      firstName: true,
      lastName: true,
      income: true,
    },
  });

  res.json(result);
});

app.get("/problems/4", async (req, res) => {
  const london = await prisma.owns.findMany({
    select: {
      customerID: true
    },
    where: {
      Account: {
        Branch: {
          branchName: 'London'
        }
      }
    },
  });

  const latveria = await prisma.owns.findMany({
    select: {
      customerID: true
    },
    where: {
      Account: {
        Branch: {
          branchName: 'Latveria'
        }
      }
    },
  });

  const london_and_latveria = london.filter(londonCustomer => 
    latveria.some(latveriaCustomer => latveriaCustomer.customerID == londonCustomer.customerID))
    .map(sharedCustomer => sharedCustomer.customerID);

  const result = await prisma.customer.findMany({
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          accNumber: true,
          Account: {
            select: {
              branchNumber: true
            }
          }
        }
      }
    },
    where: {
      income: {
        gt: 80000
      },
      customerID: {
        in: london_and_latveria
      }
    }
  });

  const formattedResult = result.flatMap(customer => 
    customer.Owns.map(owns => ({
      customerID: customer.customerID,
      income: customer.income,
      accNumber: owns.accNumber,
      branchNumber: owns.Account.branchNumber,
    }))
  );
  
  res.json(formattedResult.slice(0, 10));
  
  // res.json(result.slice(0, 10));
});
