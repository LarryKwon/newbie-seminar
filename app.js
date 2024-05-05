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

app.get("/problems/5", async (req, res) => {
  const BUS_and_SAV = await prisma.account.findMany({
    select: {
      accNumber: true,
      type: true,
      balance: true,
      Owns: {
        select: {
          customerID: true,
        },
      },
    },
    where: {
      type: {
        in: ["BUS", "SAV"],
      },
    },
  });

  const formattedaccounts = BUS_and_SAV.flatMap(accounts => 
    accounts.Owns.map(owns => ({
      customerID: owns.customerID,
      type: accounts.type,
      accNumber: accounts.accNumber,
      balance: accounts.balance
    }))
  );

  formattedaccounts.sort((a, b) => {
    // 먼저 customerID로 정렬
    if (a.customerID !== b.customerID) {
      return a.customerID - b.customerID;
    } 
    // 그 다음 type로 정렬
    else if (a.type !== b.type) {
      return a.type.localeCompare(b.type);
    } 
    // 마지막으로 accNumber로 정렬
    else {
      return a.accNumber - b.accNumber;
    }
  });
  
  res.json(formattedaccounts.slice(0, 10));
});

app.get("/problems/6", async (req, res) => {
  const managerPhilip = await prisma.account.findMany({
    // join
    include: {
      Branch: {
        select: {
          branchNumber: true,
          branchName: true,
        },
      },
    },
    where: {
      Branch: {
        Employee_Branch_managerSINToEmployee: {
          firstName: "Phillip",
          lastName: "Edwards",
        }
      }
    },
  });

  const filter_accounts = managerPhilip.filter((account) => parseFloat(account.balance) > 100000)
  const map_accounts = filter_accounts.map(e => ({
    branchName: e.Branch.branchName,
    accNumber: e.accNumber,
    balance: e.balance
  }))

  const formattedaccounts = map_accounts.sort((a, b) => {
    a.accNumber - b.accNumber
  });

  res.json(formattedaccounts.slice(0, 10));
});

app.get("/problems/7", async (req, res) => {
  const london = await prisma.customer.findMany({
    select: {
      Owns: true
    },
    where: {
      Owns: {
        some: {
          Account:{
            Branch: {
              branchName: 'London'
            }
          }
        }
      }
    },
  });

  const london_flat = london.flatMap(accounts => accounts.Owns.map(o => ({customerID: o.customerID, accNumber: o.accNumber})));

  const NYbutnotLondon = await prisma.customer.findMany({
    select: {
      Owns: true
    },
    where: {
      AND: [
        {
        Owns: {
          some: {
            Account:{
              Branch: {
                branchName: 'New York'
              }
            }
          }
        }
        },
        {
          NOT: {
          Owns: {
            some: {
              Account:{
                Branch: {
                  branchName: 'London'
                }
              }
            }
          },
        }
      }
      ]
    },
  });

  const NYbutnotLondon_flat = NYbutnotLondon.flatMap(account => account.Owns.map(own => ({customerID: own.customerID, accNumber: own.accNumber})));

  // 런던에 소유권이 없어야 함
  // do not co-own an account with another customer who owns an account at the London branch
  const filter_accounts = NYbutnotLondon_flat.filter(account => london_flat.some(london_a => (london_a.customerID != account.customerID) && (london_a.accNumber == account.accNumber)));

  const filterID = filter_accounts.map(account => account.customerID);

  const filtered_accounts = await prisma.customer.findMany({
    select: {
      customerID: true
    },
    where: {
      AND: [
        {
        Owns: {
          some: {
            Account:{
              Branch: {
                branchName: 'New York'
              }
            }
          }
        }
        },
        {
          NOT: {
          Owns: {
            some: {
              Account:{
                Branch: {
                  branchName: 'London'
                }
              }
            }
          },
        }
      },
      {
        NOT: {
          customerID: { 
            in: filterID
          }
        }
      }
      ]
    },
    orderBy: {
      customerID: 'asc'
    },
    take: 10
  });

  res.json(filtered_accounts.slice(0, 10));
});



