const express = require("express");
const { PrismaClient } = require("@prisma/client");
/**
 * prisma를 이용한 express 서버를 짤 때는 typescript로 하는 것이 좋습니다.
 * prisma의 장점이, 정의된 타입을 기반으로 객체 탐색하듯이 쿼리를 함수처럼 짤 수 있다는 것인데, javascript로 하면 그게 어렵습니다.
 * 타입이 맞춰지는 typescript를 권장합니다.
 * @type {*|Express}
 */



const app = express();
app.use(express.json());
const prisma = new PrismaClient({
  log: ['query', 'info', 'warn', 'error'],
});
prisma.$on('query', e => {
  console.log('Query: ' + e.query)
  console.log('Params: ' + e.params)
  console.log('Duration: ' + e.duration + 'ms')
})

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


/**
 * 4번 같은 경우에 london, latveria 이거 두 개를 sequential 하게 가져오는 식으로 짜셨는데, 비즈니스 로직에서 이런 경우로 짜시면 DB Select가 동기적으로 일어나서 좀 느리고요.
 * 좋은 방법은 비동기로 두 개를 한 꺼번에 들고오는 겁니다.
 * await Promise.all([london, latveria]) 이런 식으로 Promise.all을 쓰면 됩니다.
 * const london, latveria = await Promise.all([
 * prisma.owns.findMany({}), prisma.owns.findmany({})])
 * 아마 이런 식으로 짜면 되는 걸로 아는데, 갑자기 쓰려니까 잘 기억이 안 나네요.
 * otl 서버(nest버전) 코드에 이런 식의 접근이 많이 있으니 참고해보세요!
 *
 */
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

app.get("/problems/8", async (req, res) => {
  const manager = await prisma.branch.findMany({
    select: {
      managerSIN: true
    }
  });
  
  const employees = await prisma.employee.findMany({
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
      Branch_Employee_branchNumberToBranch: {
        select: {
          branchName: true
        }
      }
    },
    where: {
      salary: {
        gt: 50000
      }
    },
  });

  const temp = employees.map(employee => ({
    sin: employee.sin,
    firstName: employee.firstName,
    lastName: employee.lastName,
    salary: employee.salary,
    branchName: employee.Branch_Employee_branchNumberToBranch.branchName
  })); 

  const not_null = temp.filter(employee => 
    manager.some(emp => 
      emp.managerSIN == employee.sin));
  
  const is_null = temp.map(employee => {
    if (!manager.some(emp => 
      emp.managerSIN == employee.sin)) {
      return ({
        sin: employee.sin,
        firstName: employee.firstName,
        lastName: employee.lastName,
        salary: employee.salary,
        branchName: null
      })
    }
  });

  const is_manager = not_null.sort((a, b) => {
    if (a.branchName > b.branchName) return -1;
    else if (a.branchName < b.branchName) return 1;
    else  return 0;
  });

  const not_manager = is_null.sort((a, b) => {
    if (a.firstName < b.firstName) return -1;
    else if (a.firstName > b.firstName) return 1;
    else  return 0;
  });

  const full_list = is_manager.concat(not_manager);

  res.json(full_list.slice(0, 10));
});

/**
 * 얘도 동시에 manager랑 employee를 들고오면 좋겠죠?
 */
app.get("/problems/9", async (req, res) => {
  const manager = await prisma.branch.findMany({
    select: {
      managerSIN: true
    }
  });
  
  const employees = await prisma.employee.findMany({
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
      Branch_Employee_branchNumberToBranch: {
        select: {
          branchName: true
        }
      }
    },
    where: {
      salary: {
        gt: 50000
      }
    },
  });

  const temp = employees.map(employee => ({
    sin: employee.sin,
    firstName: employee.firstName,
    lastName: employee.lastName,
    salary: employee.salary,
    branchName: employee.Branch_Employee_branchNumberToBranch.branchName
  })); 

  const not_null = temp.filter(employee => 
    manager.some(emp => 
      emp.managerSIN == employee.sin));
  
  const is_null = temp.map(employee => {
    if (!manager.some(emp => 
      emp.managerSIN == employee.sin)) {
      return ({
        sin: employee.sin,
        firstName: employee.firstName,
        lastName: employee.lastName,
        salary: employee.salary,
        branchName: null
      })
    }
  });

  const is_manager = not_null.sort((a, b) => {
    if (a.branchName > b.branchName) return -1;
    else if (a.branchName < b.branchName) return 1;
    else  return 0;
  });

  const not_manager = is_null.sort((a, b) => {
    if (a.firstName < b.firstName) return -1;
    else if (a.firstName > b.firstName) return 1;
    else  return 0;
  });

  const full_list = is_manager.concat(not_manager);

  res.json(full_list.slice(0, 10));
});

app.get("/problems/10", async (req, res) => {
  const result = await prisma.employee.findMany({
    take: 10,
  })
  res.json(result);
});

app.get("/problems/11", async (req, res) => {
  const min_salary = await prisma.employee.findMany({
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: "Berlin",
      },
    },
    orderBy: {
      salary: "asc",
    },
    take: 1,
  });

  const min = min_salary[0].salary;

  const employees = await prisma.employee.findMany({
    where: {
      salary: min,
      Branch_Employee_branchNumberToBranch: {
        branchName: "Berlin",
      },
    },
    orderBy: {
      sin: "asc",
    },
    take: 10,
  });

  const result = employees.map((employee) => ({
    sin: employee.sin,
    firstName: employee.firstName,
    lastName: employee.lastName,
    salary: employee.salary,
  }));

  res.json(result);
});

app.get("/problems/14", async (req, res) => {
  const sumOfSalaries = await prisma.employee.aggregate({
    _sum: {
      salary: true,
    },
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: 'Moscow',
      },
    },
  });  

  const result = {
    "sum of employees salaries": sumOfSalaries._sum.salary.toString(),
  };
  res.json(result);
});

app.get("/problems/15", async (req, res) => {
  const full_join = await prisma.customer.findMany({
    // join
    include: {
      Owns: {
        include: {
          Account: {
            include: {
              Branch: true,
            },
          },
        },
      },
    },
  });

  const result = full_join.filter((customer) => {
      // distinct
      const branchNames = new Set(
        customer.Owns.map((branch) => branch.Account.Branch.branchName)
      );
      return branchNames.size === 4;
    })
  
  const filter_result = result.map((customer) => ({
    customerID: customer.customerID,
    firstName: customer.firstName,
    lastName: customer.lastName,
  }));
    
  const result_branch = filter_result.sort((a, b) => {
    if (a.branchName > b.branchName) return -1;
    else if (a.branchName < b.branchName) return 1;
    else  return 0;
  });

  const result_N = result_branch.sort((a, b) => {
    if (a.lastName < b.lastName) return -1;
    else if (a.lastName > b.lastName) return 1;
    else  return 0;
  });
  res.json(result_N.slice(0, 10));
});

app.get("/problems/17", async (req, res) => {
  const customers = await prisma.customer.findMany({
    include: {
      Owns: {
        include: {
          Account: true
        }
      }
    },
    where: {
      lastName: {
        startsWith: 'S',
        contains: 'e'
      }
    }
  });

  const filter = customers.filter(account => {
    const num_accounts = new Set(account.Owns.map(own => own.accNumber));
    return num_accounts.size >= 3;
  });

  const filter2 = filter.map(account => {
    const sum_balance = account.Owns.reduce((sum, balan) => {
      return sum + parseFloat(balan.Account.balance);
    }, 0);

    const average_balance = sum_balance / account.Owns.length;
    return {
      customerID: account.customerID,
      firstName: account.firstName,
      lastName: account.lastName,
      income: account.income,
      'average account balance': average_balance 
    };
  });
    
  const result = filter2.sort((a, b) => {
    if (a.customerID < b.customerID) return -1;
    else if (a.customerID > b.customerID) return 1;
    else  return 0;
  });

  res.json(result.slice(0, 10));
});

app.get("/problems/18", async (req, res) => {
  const accounts = await prisma.account.findMany({
    include: {
      Transactions: true,
    },
    where: {
      Branch: {
        branchName: "Berlin",
      }
    }
  });

  const filter = accounts.filter(account => {
    return account.Transactions.length >= 10;
  });

  const filter2 = filter.map(account => {
    const sum_trans = account.Transactions.reduce((sum, tran) => {
      return sum + parseFloat(tran.amount);
    }, 0);
    return {
      accNumber: account.accNumber,
      balance: account.balance,
      sum: sum_trans
    }
  });
    
  const result = filter2.sort((a, b) => {
    if (a.sum < b.sum) return -1;
    else if (a.sum > b.sum) return 1;
    else  return 0;
  });

  res.json(result.slice(0, 10));
});

app.post("/employee/join", async (req, res) => {
  try {
    const { sin, firstName, lastName, salary, branchNumber } = req.body;

    const newEmployee = await prisma.employee.create({
      data: {
        sin: sin,
        firstName: firstName,
        lastName: lastName,
        salary: salary,
        branchNumber: branchNumber,
      },
    });

    res.status(201).json({ createdUser: newEmployee });
  } catch (error) {
    console.error("Error adding new employee: ", error);
    res.status(500).send("Failed to add new employee.");
  }
});

app.delete("/employee/leave", async (req, res) => {
  try {
    const { sin } = req.body;
    const deletedEmployee = await prisma.employee.delete({
      where: {
        sin: sin
      }
    });

    // Send a success response
    res.status(200).json({ message: "Employee successfully deleted", deletedEmployee });
  } catch (error) {
    console.error("Error deleting employee: ", error);
    res.status(500).json({ error: "Failed to delete employee" });
  }
});

app.post("/account/:account_no/deposit", async (req, res) => {
  const { account_no } = req.params;
  const { customerID, firstName, lastName, amount } = req.body;

  if (amount < 0) {
    return res.status(400).send({ error: "Invalid amount. Deposit amount must be positive." });
  }

  try {
    const account = await prisma.account.findUnique({
      where: { accNumber: account_no },
      include: { Owns: true },
    });
    
    if (!account) {
      return res.status(404).send({ error: "Account not found." });
    }

    const name_match_account = account.Owns.some(
      (own) => own.customerID === customerID
    );
    if (!name_match_account) {
      return res.status(403).send({error: "Unauthorized: Customer does not own this account."});
    }

    const deposit = await prisma.account.update({
      select: { balance: true },
      where: { accNumber: account_no },
      data: { balance: (parseFloat(account.balance) + parseFloat(amount)).toString() },
    });

    res.status(200).send({ message: "Deposit successful", balance: deposit.balance });
  } catch (error) {
    console.error(error);
    res.status(500).send({ error: "Internal Server Error" });
  }
});

app.post("/account/:account_no/withdraw", async (req, res) => {
  const { account_no } = req.params;
  const { customerID, firstName, lastName, amount } = req.body;

  if (amount < 0) {
    return res.status(400).send({ error: "Invalid amount. Deposit amount must be positive." });
  }

  try {
    const account = await prisma.account.findUnique({
      where: { accNumber: account_no },
      include: { Owns: true },
    });
    
    if (!account) {
      return res.status(404).send({ error: "Account not found." });
    }

    const name_match_account = account.Owns.some(
      (own) => own.customerID === customerID
    );
    if (!name_match_account) {
      return res.status(403).send({error: "Unauthorized: Customer does not own this account."});
    }

    if (account.balance < amount) {
      return res.status(400).send("Insufficient funds for withdrawal.");
    }

    const withdrawal = await prisma.account.update({
      select: { balance: true },
      where: { accNumber: account_no },
      data: { balance: (parseFloat(account.balance) - parseFloat(amount)).toString() },
    });

    res.status(200).send({ message: "Withdrawal successful", balance: withdrawal.balance });
  } catch (error) {
    res.status(500).send({ error: "Internal Server Error" });
  }
});