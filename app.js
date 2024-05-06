const express = require("express");
const { PrismaClient } = require("@prisma/client");

const bodyParser = require("body-parser");

const app = express();
const prisma = new PrismaClient();
const port = 3001;

/**
 * prisma를 이용한 express 서버를 짤 때는 typescript로 하는 것이 좋습니다.
 * prisma의 장점이, 정의된 타입을 기반으로 객체 탐색하듯이 쿼리를 함수처럼 짤 수 있다는 것인데, javascript로 하면 그게 어렵습니다.
 * 타입이 맞춰지는 typescript를 권장합니다.
 * @type {*|Express}
 */

app.get("/", (req, res) => {
  res.send("Successfully Connected!");
});

app.use(bodyParser.json());

// Problem 1
app.get("/problems/1", async (req, res) => {
  const customers = await prisma.customer.findMany({
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
    orderBy: [{ income: "desc" }, { lastName: "asc" }, { firstName: "asc" }],
    take: 10,
  });
  res.json(customers);
});

// Problem 2
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
        include: {
          Employee_Branch_managerSINToEmployee: true,
        },
      },
    },
  });
  const result = employees
    .map((employee) => ({
      sin: employee.sin,
      branchName: employee.Branch_Employee_branchNumberToBranch.branchName,
      salary: employee.salary,
      "Salary Diff": employee.Branch_Employee_branchNumberToBranch
        .Employee_Branch_managerSINToEmployee
        ? `${employee.Branch_Employee_branchNumberToBranch.Employee_Branch_managerSINToEmployee.salary - employee.salary}`
        : null,
    }))
    .sort((a, b) => b["Salary Diff"] - a["Salary Diff"])
    .slice(0, 10);
  res.json(result);
});

// Problem 3
app.get("/problems/3", async (req, res) => {
  const butlerIncome = await prisma.customer.findMany({
    select: {
      income: true,
    },
    where: {
      lastName: "Butler",
    },
  });
  var maxIncome = 0;
  butlerIncome.forEach((c) => {
    if (c.income && c.income * 2 > maxIncome) {
      maxIncome = c.income * 2;
    }
  });
  const result = await prisma.customer.findMany({
    select: {
      firstName: true,
      lastName: true,
      income: true,
    },
    where: {
      income: {
        gte: maxIncome,
      },
    },
    orderBy: [{ lastName: "asc" }, { firstName: "asc" }],
    take: 10,
  });
  res.json(result);
});

// Problem 4
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
  const london = await prisma.customer.findMany({
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
      Owns: {
        some: {
          Account: {
            Branch: {
              branchName: "London",
            },
          },
        },
      },
    },
  });
  const latveria = await prisma.customer.findMany({
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
      Owns: {
        some: {
          Account: {
            Branch: {
              branchName: "Latveria",
            },
          },
        },
      },
    },
  });
  const customers = london.filter((customer) => {
    return latveria.some(
      (customer2) => customer.customerID === customer2.customerID,
    );
  });
  const result = customers
    .map((customer) => {
      return customer.Owns.map((own) => {
        return {
          customerID: customer.customerID,
          income: customer.income,
          accNumber: own.accNumber,
          branchNumber: own.Account.branchNumber,
        };
      });
    })
    .flat()
    .sort((a, b) => {
      if (a.customerID !== b.customerID) return a.customerID - b.customerID;
      else return a.accNumber - b.accNumber;
    })
    .slice(0, 10);
  res.json(result);
});

// Problem 5
app.get("/problems/5", async (req, res) => {
  const accounts = await prisma.account.findMany({
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
  const result = accounts
    .map((account) =>
      account.Owns.map((owns) => ({
        customerID: owns.customerID,
        type: account.type,
        accNumber: account.accNumber,
        balance: account.balance,
      })),
    )
    .flat()
    .sort((a, b) => {
      if (a.customerID !== b.customerID) return a.customerID - b.customerID;
      else if (a.type !== b.type) return a.type.localeCompare(b.type);
      else return a.accNumber - b.accNumber;
    })
    .slice(0, 10);
  res.json(result);
});

// Problem 6
app.get("/problems/6", async (req, res) => {
  const manager = await prisma.employee.findFirst({
    select: {
      Branch_Branch_managerSINToEmployee: {
        select: {
          branchNumber: true,
        },
      },
    },
    where: {
      firstName: "Phillip",
      lastName: "Edwards",
    },
  });
  const accounts = await prisma.account.findMany({
    select: {
      Branch: {
        select: {
          branchName: true,
        },
      },
      accNumber: true,
      balance: true,
    },
    where: {
      branchNumber: manager.Branch_Branch_managerSINToEmployee.branchNumber,
    },
    orderBy: {
      accNumber: "asc",
    },
  });
  const filteredAccounts = accounts.filter(
    (account) => parseFloat(account.balance) > 100000,
  );
  const result = filteredAccounts
    .map((account) => ({
      branchName: account.Branch.branchName,
      accNumber: account.accNumber,
      balance: account.balance,
    }))
    .sort((a, b) => parseInt(a.accNumber) - parseInt(b.accNumber))
    .slice(0, 10);
  res.json(result);
});

// Problem 7
app.get("/problems/7", async (req, res) => {
  const newyork = await prisma.branch.findFirst({
    select: {
      branchNumber: true,
    },
    where: {
      branchName: "New York",
    },
  });
  const london = await prisma.branch.findFirst({
    select: {
      branchNumber: true,
    },
    where: {
      branchName: "London",
    },
  });
  const newyorkCustomers = await prisma.customer.findMany({
    select: {
      customerID: true,
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
      Owns: {
        some: {
          Account: {
            branchNumber: newyork?.branchNumber,
          },
        },
      },
    },
  });
  const londonCustomers = await prisma.customer.findMany({
    select: {
      customerID: true,
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
      Owns: {
        some: {
          Account: {
            branchNumber: london?.branchNumber,
          },
        },
      },
    },
  });
  const customers = await prisma.customer.findMany({
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
      AND: [
        {
          customerID: {
            in: newyorkCustomers.map((customer) => customer.customerID),
          },
        },
        {
          NOT: {
            customerID: {
              in: londonCustomers.map((customer) => customer.customerID),
            },
          },
        },
      ],
    },
  });
  const customers2 = await prisma.customer.findMany({
    select: {
      customerID: true,
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
      Owns: {
        some: {
          accNumber: {
            in: londonCustomers
              .map((customer) => {
                return customer.Owns.map((own) => own.accNumber);
              })
              .flat(),
          },
        },
      },
    },
  });

  const id = customers
    .map((customer) => customer.customerID)
    .filter(
      (customerID) =>
        !customers2.map((customer) => customer.customerID).includes(customerID),
    );
  const result = id
    .map((customerID) => {
      return {
        customerID: customerID,
      };
    })
    .sort((a, b) => a.customerID - b.customerID)
    .slice(0, 10);
  res.json(result);
});

// Problem 8
app.get("/problems/8", async (req, res) => {
  const employees = await prisma.employee.findMany({
    where: {
      salary: {
        gt: 50000,
      },
    },
    include: {
      Branch_Branch_managerSINToEmployee: true,
    },
    orderBy: {
      firstName: "asc",
    },
  });
  const result = employees
    .map((employee) => ({
      sin: employee.sin,
      firstName: employee.firstName,
      lastName: employee.lastName,
      salary: employee.salary,
      branchName:
        employee.Branch_Branch_managerSINToEmployee.length > 0
          ? employee.Branch_Branch_managerSINToEmployee[0].branchName
          : null,
    }))
    .sort((a, b) => {
      if (a.branchName !== b.branchName) {
        if (a.branchName === null) return 1;
        else if (b.branchName === null) return -1;
        else return b.branchName.localeCompare(a.branchName);
      } else {
        return a.firstName.localeCompare(b.firstName);
      }
    })
    .slice(0, 10);
  res.json(result);
});

// Problem 9
app.get("/problems/9", async (req, res) => {
  const employees = await prisma.employee.findMany({
    where: {
      salary: {
        gt: 50000,
      },
    },
    include: {
      Branch_Branch_managerSINToEmployee: true,
    },
    orderBy: {
      firstName: "asc",
    },
  });
  const result = employees
    .map((employee) => ({
      sin: employee.sin,
      firstName: employee.firstName,
      lastName: employee.lastName,
      salary: employee.salary,
      branchName:
        employee.Branch_Branch_managerSINToEmployee.length > 0
          ? employee.Branch_Branch_managerSINToEmployee[0].branchName
          : null,
    }))
    .sort((a, b) => {
      if (a.branchName !== b.branchName) {
        if (a.branchName === null) return 1;
        else if (b.branchName === null) return -1;
        else return b.branchName.localeCompare(a.branchName);
      } else {
        return a.firstName.localeCompare(b.firstName);
      }
    })
    .slice(0, 10);
  res.json(result);
});

// Problem 10
app.get("/problems/10", async (req, res) => {
  const target = await prisma.owns.findMany({
    where: {
      Customer: {
        firstName: "Helen",
        lastName: "Morgan",
      },
    },
    include: {
      Account: true,
    },
  });
  const branchNum = new Set(target.map((own) => own.Account.branchNumber));
  const customers = await prisma.customer.findMany({
    where: {
      income: {
        gt: 5000,
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
  const filteredCustomers = customers.filter((customer) => {
    const tmpBranchNum = new Set(
      customer.Owns.map((own) => own.Account.branchNumber),
    );
    return [...branchNum].every((branch) => tmpBranchNum.has(branch));
  });
  const result = filteredCustomers
    .map((customer) => ({
      customerID: customer.customerID,
      firstName: customer.firstName,
      lastName: customer.lastName,
      income: customer.income,
    }))
    .sort((a, b) => b.income - a.income)
    .slice(0, 10);
  res.json(result);
});

// Problem 11
app.get("/problems/11", async (req, res) => {
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

// Problem 14
app.get("/problems/14", async (req, res) => {
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

// Problem 15
/**
 * 해당 부분은 include를 중복으로 사용하시는데, 실전에서는 3중 Join부터는 조심할 필요가 있습니다.
 * 따로따로 들고와서 APP Level에서 합치는게 나을 수도 있어요.
 * 굳이 지금 로직에서 당장 필요한게 아니라면, 나중에 필요할 때만 탐색하는 것도 방법입니다.
 */
app.get("/problems/15", async (req, res) => {
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
      customer.Owns.map((own) => own.Account.Branch.branchName),
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
      if (a.lastName !== b.lastName)
        return a.lastName.localeCompare(b.lastName);
      else return a.firstName.localeCompare(b.firstName);
    })
    .slice(0, 10);
  res.json(result);
});

// Problem 17
app.get("/problems/17", async (req, res) => {
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
          acc + parseFloat(cur.Account.balance) / array.length,
        0,
      ),
    }))
    .sort((a, b) => a.customerID - b.customerID)
    .slice(0, 10);
  res.json(result);
});

// Problem 18
app.get("/problems/18", async (req, res) => {
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
        (acc, cur, idx) => acc + parseFloat(cur.amount),
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

// Implement employee/join
app.post("/employee/join", async (req, res) => {
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
  res.status(201)
    .send(`'이 팀은 미친듯이 일하는 일꾼들로 이루어진 광전사 설탕 노움 조합이다.
분위기에 적응하기는 쉽지 않지만 아주 화력이 좋은 강력한 조합인거 같다.'`);
});

// Implement employee/leave
app.delete("/employee/leave", async (req, res) => {
  const { sin } = req.body;
  if (sin) {
    const sinNumber = parseInt(sin);
    const exists = await prisma.employee.findUnique({
      where: { sin: sinNumber },
    });
    if (exists) {
      await prisma.employee.delete({
        where: { sin: sinNumber },
      });
      res
        .status(200)
        .send(
          "안녕히 계세요 여러분!\n" +
            "전 이 세상의 모든 굴레와 속박을 벗어 던지고 제 행복을 찾아 떠납니다!\n" +
            "여러분도 행복하세요~~!",
        );
    }
  }
});

// Implement account/{account_no}/deposit
app.post("/account/:account_no/deposit", async (req, res) => {
  const { account_no } = req.params;
  const { customerID, amount } = req.body;
  const accountNo = parseInt(account_no);
  const account = await prisma.account.findUnique({
    where: { accNumber: accountNo },
    include: { Owns: true },
  });
  const isOwner = account.Owns.some((own) => own.customerID === customerID);
  if (isOwner && parseFloat(amount) > 0) {
    const newBalance = parseFloat(account.balance) + parseFloat(amount);
    const newAccount = await prisma.account.update({
      where: { accNumber: accountNo },
      data: {
        balance: newBalance.toString(),
      },
    });
    res.status(200).send(newAccount.balance);
  }
});

// Implement account/{account_no}/withdraw
app.post("/account/:account_no/withdraw", async (req, res) => {
  const { account_no } = req.params;
  const { customerID, amount } = req.body;
  const accountNo = parseInt(account_no);
  const account = await prisma.account.findUnique({
    where: { accNumber: accountNo },
    include: { Owns: true },
  });
  const isOwner = account.Owns.some((own) => own.customerID === customerID);
  if (isOwner && parseFloat(amount) > 0) {
    const newBalance = parseFloat(account.balance) + parseFloat(amount);
    if (newBalance >= 0) {
      const newAccount = await prisma.account.update({
        where: { accNumber: accountNo },
        data: {
          balance: newBalance.toString(),
        },
      });
      res.status(200).send(newAccount.balance);
    }
  }
});

app.listen(port, () => {
  console.log(`Server: http://localhost:${port}`);
});
