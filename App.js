const express = require("express");
const app = express();
app.use(express.json());
const PORT = 3000;
/**
 * prisma를 이용한 express 서버를 짤 때는 typescript로 하는 것이 좋습니다.
 * prisma의 장점이, 정의된 타입을 기반으로 객체 탐색하듯이 쿼리를 함수처럼 짤 수 있다는 것인데, javascript로 하면 그게 어렵습니다.
 * 타입이 맞춰지는 typescript를 권장합니다.
 * @type {*|Express}
 */


/**
 *
 * 실전에서는
 * 3중 join 정도로 깊어지면, subquery 쓰는 걸 고려해봄직 합니다. 지금 3중 join이 없어서 딱히 드릴 말씀은 없네요!
 */
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

/**
 * 4번 같은 경우에 london, latveria 이거 두 개를 sequential 하게 가져오는 식으로 짜셨는데, 비즈니스 로직에서 이런 경우로 짜시면 DB Select가 동기적으로 일어나서 좀 느리고요.
 * 좋은 방법은 비동기로 두 개를 한 꺼번에 들고오는 겁니다.
 * await Promise.all([london, latveria]) 이런 식으로 Promise.all을 쓰면 됩니다.
 * const london, latveria = await Promise.all([
 * prisma.owns.findMany({}), prisma.owns.findmany({})])
 * 아마 이런 식으로 짜면 되는 걸로 아는데, 갑자기 쓰려니까 잘 기억이 안 나네요.
 * otl 서버(nest버전) 코드에 이런 식의 접근이 많이 있으니 참고해보세요!
 *
 **/

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

/**
 *
 * prisma로 작업하시는 거는 전반적으로 문제될 게 없어서 리뷰는 따로 안 남겼고요.
 * 아랫 부분 정도만 추가로 생각해보시면 좋을 거 같아 남깁니다.
 * 1. deposit은 누구나 할 수 있습니다.
 * 생각해보면 제가 남에게 송금할 때, 다른 사람 계좌에 마음대로 보낼 수 있잖아요? 지금은 deposit을 자신만 할 수 있다고 강력하게 걸어놨는데, 이게 풀리면 어떻게 될까요?
 * 2. deposit이 누구나 가능하다고 하면 다음과 같은 일이 일어날 수 있습니다.
 *
 * 상황 A.
 * 현재 4000원임
 * A가 3000원을 출금하려고 함
 * B는 2000원을 입금함.
 * 모든 일이 일어나고 보니 잔고가 3000원이 아님
 *
 * A는 해당 계좌에 lock을 걸어서 해결할 수 있습니다. lock과 transaction을 걺으로써, 해당 A와 B의 순서를 sequential하게 맞춰주면 이런 일이 발생하지 않습니다.
 */


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

