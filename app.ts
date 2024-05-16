import express, { Request, Response } from "express";
import { PrismaClient } from "@prisma/client";
import { orderBy } from "lodash";
import { Prisma } from '@prisma/client';

type CustomOwnsWhereInput = Omit<Prisma.OwnsWhereInput, 'Account'> & {
  Account?: Prisma.AccountWhereInput & {
    Customer?: {
      firstName: string;
      lastName: string;
    };
  };
};


const app = express();
app.use(express.json());
const PORT = 3000;
const prisma = new PrismaClient();

app.get("/", (req: Request, res: Response) => {
  res.send("get");
});

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

app.get("/problems/1", async (req: Request, res: Response) => {
  try {
    const result = await prisma.customer.findMany({
      select: {
        firstName: true,
        lastName: true,
        income: true,
      },
      where: {
        income: {
          gte: 50000,
          lte: 60000,
        },
      },
      orderBy: [
        { income: 'desc' },
        { lastName: 'asc' },
        { firstName: 'asc' },
      ],
      take: 10,
    });
    res.json(result);
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Internal Server Error");
  }
});
app.get("/problems/4", async (req: Request, res: Response) => {
  try {
    const london = await prisma.owns.findMany({
      select: {
        customerID: true,
      },
      where: {
        Account: {
          Branch: {
            branchName: 'London',
          },
        },
      },
    });

    const latveria = await prisma.owns.findMany({
      select: {
        customerID: true,
      },
      where: {
        Account: {
          Branch: {
            branchName: 'Latveria',
          },
        },
      },
    });

    const londonLatveria = london
      .filter(londonCustomer =>
        latveria.some(latveriaCustomer => latveriaCustomer.customerID === londonCustomer.customerID))
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
                branchNumber: true,
              },
            },
          },
        },
      },
      where: {
        income: {
          gt: 80000,
        },
        customerID: {
          in: londonLatveria,
        },
      },
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
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Internal Server Error");
  }
});

app.get("/problems/6", async (req: Request, res: Response) => {
  try {
    const managerPhilip = await prisma.account.findMany({
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
          },
        },
      },
    });

    const filter_accounts = managerPhilip.filter(account => account.balance !== null && parseFloat(account.balance) > 100000);
    const map_accounts = filter_accounts.map(account => ({
      branchName: account.Branch?.branchName || 'Unknown', // Optional Chaining 사용하여 null 체크
      accNumber: account.accNumber,
      balance: account.balance || 'Unknown',
    }));
    

    const formattedAccounts = map_accounts.sort((a, b) => a.accNumber - b.accNumber);

    res.json(formattedAccounts.slice(0, 10));
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Internal Server Error");
  }
});


app.get("/problems/7", async (req: Request, res: Response) => {
  try {
    const newyork = await prisma.owns.findMany({
      select: {
        customerID: true,
      },
      where: {
        Account: {
          Branch: {
            branchName: 'New York',
          },
        },
      },
    });

    const london = await prisma.owns.findMany({
      select: {
        customerID: true,
      },
      where: {
        Account: {
          Branch: {
            branchName: 'London',
          },
        },
      },
    });

    const london_customers = london.map(londonCustomer => londonCustomer.customerID);
    const newyork_customers = newyork.map(newyorkCustomer => newyorkCustomer.customerID);
    const result = newyork_customers.filter(newyorkCustomer =>
      !london_customers.includes(newyorkCustomer)
    );

    res.json(result.slice(0, 10));
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Internal Server Error");
  }
});


app.get("/problems/8", async (req: Request, res: Response) => {
  const employees = await prisma.employee.findMany({
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
      Branch_Branch_managerSINToEmployee: {
        select: {
          branchName: true
        }
      }
    },
    where: {
      salary: {
        gt: 50000
      },
    },
  });

  const formattedEmployees = employees.map((employee) => ({
    sin: employee.sin,
    firstName: employee.firstName,
    lastName: employee.lastName,
    salary: employee.salary,
    branchName: employee.Branch_Branch_managerSINToEmployee.length > 0 ? employee.Branch_Branch_managerSINToEmployee[0].branchName : null,
  }));

  formattedEmployees.sort((a, b) => {
    if (a.branchName && b.branchName) {
      return a.branchName.localeCompare(b.branchName);
    }
    return 0;
  });

  res.json(formattedEmployees.slice(0, 10));
});

app.get("/problems/9", async (req: Request, res: Response) => {
  const employees = await prisma.employee.findMany({
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
      Branch_Branch_managerSINToEmployee: {
        select: {
          branchName: true
        }
      }
    },
    where: {
      salary: {
        gt: 50000
      },
    },
  });

  const formattedEmployees = employees.map((employee) => ({
    sin: employee.sin,
    firstName: employee.firstName,
    lastName: employee.lastName,
    salary: employee.salary,
    branchName: employee.Branch_Branch_managerSINToEmployee.length > 0 ? employee.Branch_Branch_managerSINToEmployee[0].branchName : null,
  }));

  formattedEmployees.sort((a, b) => {
    if (a.branchName && b.branchName) {
      return a.branchName.localeCompare(b.branchName);
    }
    return 0;
  });

  res.json(formattedEmployees.slice(0, 10));
});

app.get("/problems/10", async (req: Request, res: Response) => {


  const helenMorganAccounts = await prisma.owns.findMany({
    select: {
      accNumber: true
    },
    where: {
      AND: [
        {
          Account: {
            Customer: {
              firstName: "Helen",
              lastName: "Morgan"
            }
          }
        }
      ]
    } as CustomOwnsWhereInput
  });
  
  

  const helenMorganBranches = await prisma.account.findMany({
    select: {
      branchNumber: true
    },
    where: {
      accNumber: {
        in: helenMorganAccounts.map(account => account.accNumber)
      }
    }
  });

  const helenMorganBranchNumbers = helenMorganBranches.map(branch => branch.branchNumber);

  const filteredBranchNumbers = helenMorganBranchNumbers.filter(num => num !== null) as number[];

const filteredCustomers = await prisma.customer.findMany({
  select: {
    customerID: true,
    firstName: true,
    lastName: true,
    income: true,
    Owns: {
      select: {
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
      gt: 5000
    },
    Owns: {
      every: {
        Account: {
          branchNumber: {
            in: filteredBranchNumbers
          }
        }
      }
    }
  },
  orderBy: {
    income: 'desc'
  }
});


  res.json(filteredCustomers.slice(0, 10));
});


app.get("/", (req: Request, res: Response) => {
  res.send("Server is running");
});


app.get("/problems/11", async (req: Request, res: Response) => {
  const minSalaryEmployee = await prisma.employee.findMany({
    select: {
      salary: true,
    },
    where: {
      Branch_Employee_branchNumberToBranch: { branchName: "Berlin" },
    },
    orderBy: {
      salary: "asc",
    },
    take: 1,
  });
  const minSalary = minSalaryEmployee[0].salary;
  const employees = await prisma.employee.findMany({
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
    },
    where: {
      salary: minSalary,
      Branch_Employee_branchNumberToBranch: { branchName: "Berlin" },
    },
    orderBy: {
      sin: "asc",
    },
    take: 10,
  });
  res.json(employees);
});

app.get("/problems/14", async (req: Request, res: Response) => {
  const salarySum = await prisma.employee.aggregate({
    _sum: {
      salary: true,
    },
    where: {
      Branch_Employee_branchNumberToBranch: { branchName: "Moscow" },
    },
  });
  const result = {
    "sum of employees salaries": `${salarySum._sum.salary}`,
  };
  res.json(result);
});

app.get("/problems/15", async (req: Request, res: Response) => {
  const customers = await prisma.customer.findMany({
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
  const filteredCustomers = customers.filter((customer) => {
    const branchCount = new Set(
      customer.Owns.map((own) => own.Account.Branch?.branchName),
    );
    return branchCount.size === 4;
  });
  
  const result = filteredCustomers
    .map((customer) => ({
      customerID: customer.customerID,
      firstName: customer.firstName,
      lastName: customer.lastName,
    }))
    .sort((a, b) => {
      const lastNameComparison = (a.lastName ?? '').localeCompare(b.lastName ?? '');
      if (lastNameComparison !== 0) {
        return lastNameComparison;
      } else {
        return (a.firstName ?? '').localeCompare(b.firstName ?? '');
      }
    })
    
    .slice(0, 10);
  res.json(result);
});

app.get("/problems/17", async (req: Request, res: Response) => {
  const customers = await prisma.customer.findMany({
    where: {
      lastName: {
        startsWith: "S",
        contains: "e",
      },
    },
    include: {
      Owns: {
        include: {
          Account: true,
        },
      },
    },
  });
  const result = customers
    .filter((customer) => {
      const accounts = new Set(customer.Owns.map((own) => own.accNumber));
      return accounts.size >= 3;
    })
    .map((customer) => ({
      customerID: customer.customerID,
      firstName: customer.firstName,
      lastName: customer.lastName,
      income: customer.income,
      "average account balance": customer.Owns.reduce(
        (acc, cur, idx, array) =>
          acc + (parseFloat(cur.Account.balance ?? '0') / array.length),
        0,
      ),
      
    }))
    .sort((a, b) => a.customerID - b.customerID)
    .slice(0, 10);
  res.json(result);
});

app.get("/problems/18", async (req: Request, res: Response) => {
  const accounts = await prisma.account.findMany({
    where: {
      Branch: {
        branchName: "Berlin",
      },
    },
    include: {
      Transactions: true,
      Branch: true,
    },
  });
  const filteredAccounts = accounts
    .filter((account) => account.Transactions.length >= 10)
    .map((account) => ({
      accNumber: account.accNumber,
      balance: account.balance,
      sumOfTransactionAmounts: account.Transactions.reduce(
        (acc, cur, idx) => acc + parseFloat(cur.amount ?? '0'),
        0,
      ),
      
    }))
    .sort((a, b) => a.sumOfTransactionAmounts - b.sumOfTransactionAmounts)
    .slice(0, 10);
  const result = filteredAccounts.map((account) => ({
    accNumber: account.accNumber,
    balance: account.balance,
    "sum of transaction amounts": account.sumOfTransactionAmounts,
  }));
  res.json(result);
});


