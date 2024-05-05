const express = require("express");
const app = express();
app.use(express.json());
const PORT = 3000;

const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

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
  const newyork = await prisma.owns.findMany({
    select: {
      customerID: true
    },
    where: {
      Account: {
        Branch: {
          branchName: 'New York'
        }
      }
    },
  });

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

  const london_customers = london.map(londonCustomer => londonCustomer.customerID);
  const newyork_customers = newyork.map(newyorkCustomer => newyorkCustomer.customerID);
  const result = newyork_customers.filter(newyorkCustomer => 
    !london_customers.includes(newyorkCustomer));
  
  res.json(result.slice(0, 10));
});

app.get("/problems/8", async (req, res) => {
  const employeesManager = await prisma.employee.findMany({
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
      Branch: {
        select: {
          branchName: true
        }
      }
    },
    where: {
      salary: {
        gt: 50000
      },
      Branch: {
        managerSIN: {
          not: null
        }
      }
    },
    orderBy: {
      Branch: {
        branchName: 'desc'
      },
      firstName: 'asc'
    }
  });

  const employeesNotManager = await prisma.employee.findMany({
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
    },
    where: {
      salary: {
        gt: 50000
      },
      Branch: {
        managerSIN: {
          equals: null
        }
      }
    },
    orderBy: {
      firstName: 'asc'
    }
  });
});

app.post("/employee/join", async (req, res) => {
  const { sin, firstName, lastName, salary, branchNumber } = req.body;

  try {
    const newEmployee = await prisma.employee.create({
      data: {
        sin,
        firstName,
        lastName,
        salary,
        branchNumber
      }
    });
    res.send("이 팀은 미친듯이 일하는 일꾼들로 이루어진 광전사 설탕 노움 조합이다. 분위기에 적응하기는 쉽지 않지만 아주 화력이 좋은 강력한 조합인거 같다.");
  } catch (error) {
    res.status(500).send("재료를 더 들고오라 + error.message");
  }
});

app.delete("/employee/leave", async (req, res) => {
  const { sin } = req.body;

  try {
    const deleteEmployee = await prisma.employee.delete({
      where: {
        sin: sin
      }
    });

    res.send("안녕히 계세요 여러분!\n전 이 세상의 모든 굴레와 속박을 벗어 던지고 제 행복을 찾아 떠납니다!\n여러분도 행복하세요~~!");
  } catch (error) {
    res.status(500).send("너 앉아!" + error.message);
  }
});

app.post("/account/:account_no/deposit", async (req, res) => {
  const accountNumber = parseInt(req.params.account_no);
  const { customerId, amount } = req.body;

  try {
    const ownership = await prisma.owns.findUnique({
      where: {
        customerID_accNumber: {
          customerID: customerId,
          accNumber: accountNumber
        }
      }
    });

    if (!ownership) {
      return res.status(403).send("Ownership Error! You can't deposit to an account that you don't own");
    }

    const updatedAccount = await prisma.account.update({
      where: {
        accNumber: accountNumber
      },
      data: {
        balance: {
          increment: amount
        }
      }
    });

    res.send(`Current Balance: ${updatedAccount.balance}`);
  } catch (error) {
    res.status(500).send("There was an Error at handling transaction: " + error.message);
  }
});

app.post("/account/:account_no/withdraw", async (req, res) => {
  const accountNumber = parseInt(req.params.account_no);
  const { customerId, amount } = req.body;

  try {
    const ownership = await prisma.owns.findUnique({
      where: {
        customerID_accNumber: {
          customerID: customerId,
          accNumber: accountNumber
        }
      }
    });

    if (!ownership) {
      return res.status(403).send("Ownership Error! You can't withdraw from an account that you don't own");
    }

    const currentAccount = await prisma.account.findUnique({
      where: {
        accNumber: accountNumber
      }
    });

    if (!currentAccount || currentAccount.balance < amount) {
      return res.status(400).send("Insufficient Balance!");
    }

    const updatedAccount = await prisma.account.update({
      where: {
        accNumber: accountNumber
      },
      data: {
        balance: {
          decrement: amount
        }
      }
    });

    res.send(`Current Balance: ${updatedAccount.balance}`);
  } catch (error) {
    res.status(500).send("There was an Error at handling transaction: " + error.message);
  }
});

