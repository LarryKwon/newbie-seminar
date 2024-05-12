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

async function problem2() {
  const employees = await prisma.employee.findMany({
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: {
          in: ['London', 'Berlin']
        }
      }
    },
    include: {
      Branch_Employee_branchNumberToBranch: {
        include: {
          Employee_Branch_managerSINToEmployee: true
        }
      }
    }
  });

  return employees.map(e => ({
    sin: e.sin,
    branchName: e.Branch_Employee_branchNumberToBranch.branchName,
    salary: e.salary,
    salaryDiff: e.Branch_Employee_branchNumberToBranch.Employee_Branch_managerSINToEmployee.salary - e.salary
  })).sort((a, b) => b.salaryDiff - a.salaryDiff).slice(0, 10);
}

async function problem3() {
  const maxIncomeButler = await prisma.customer.aggregate({
    where: {
      lastName: 'Butler'
    },
    _max: {
      income: true
    }
  })

  return await prisma.customer.findMany({
    where: {
      income:{
        gte: 2 * maxIncomeButler._max.income
      } 
    },
    select: {
      firstName: true,
      lastName: true,
      income: true
    },
    orderBy: [
      { lastName: 'asc' },
      { firstName: 'asc' }
    ],
    take: 10
  })
}

async function problem4() {
  const selectConfig = {
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
  };

  const findCustomersByBranch = async (branchName) => {
    return prisma.customer.findMany({
      select: selectConfig,
      where: {
        income: {
          gt: 80000,
        },
        Owns: {
          some: {
            Account: {
              Branch: {
                branchName: branchName,
              },
            },
          },
        },
      },
    });
  };

  const londonCustomers = await findCustomersByBranch("London");
  const latveriaCustomers = await findCustomersByBranch("Latveria");

  const commonCustomers = londonCustomers.filter(londonCustomer =>
    latveriaCustomers.some(latveriaCustomer =>
      londonCustomer.customerID === latveriaCustomer.customerID));

  return commonCustomers
    .map(customer => customer.Owns.map(own => ({
      customerID: customer.customerID,
      income: customer.income,
      accNumber: own.accNumber,
      branchNumber: own.Account.branchNumber,
    })))
    .flat()
    .sort((a, b) => {
      if (a.customerID !== b.customerID) return a.customerID - b.customerID;
      return a.accNumber - b.accNumber;
    })
    .slice(0, 10);
}

async function problem5() {
  const customers =  await prisma.customer.findMany({
    select: {
      customerID: true,
      Owns: {
        select: {
          Account: {
            select: {
              type: true,
              accNumber: true,
              balance: true
            }
          }
        }
      }
    },
    orderBy: [
      {
        customerID: 'asc'
      }
    ]
  });

  return customers
    .map(customer => (
      customer.Owns
          .filter(account => ['BUS', 'SAV'].includes(account.Account.type))
          .map(account => ({
              customerID: customer.customerID,
              type: account.Account.type,
              accNumber: account.Account.accNumber,
              balance: account.Account.balance
          }))
    ))
    .flat()
    .slice(0,10);
}


async function problem6() {
  const phillip = await prisma.employee.findFirst ({
    where: {
      firstName: 'Phillip',
      lastName: 'Edwards'
    },
    select: {
      Branch_Branch_managerSINToEmployee: {
        select: {
          branchNumber: true
        }
      }
    }
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
      branchNumber: phillip.Branch_Branch_managerSINToEmployee.branchNumber,
    },
    orderBy: {
      accNumber: "asc",
    },
  });

  return accounts
    .filter((account) => parseFloat(account.balance) > 100000)
    .map(({ Branch, accNumber, balance }) => ({
      branchName: Branch.branchName,
      accNumber,
      balance
    }))
    .slice(0,10);
}

async function problem7 () {
  const [newyorkBranch, londonBranch] = await Promise.all([
    prisma.branch.findFirst({ where: { branchName: "New York" }, select: { branchNumber: true } }),
    prisma.branch.findFirst({ where: { branchName: "London" }, select: { branchNumber: true } })
  ]);

  const [newyorkCustomers, londonCustomers] = await Promise.all([
    prisma.customer.findMany({
      where: {
        Owns: {
          some: {
            Account: {
              branchNumber: newyorkBranch?.branchNumber,
            },
          },
        },
      },
      select: { customerID: true },  
    }),
    prisma.customer.findMany({
      where: {
        Owns: {
          some: {
            Account: {
              branchNumber: londonBranch?.branchNumber,
            },
          },
        },
      },
      select: { customerID: true },  
    })
  ]);

  const newyorkCustomerIDs = newyorkCustomers.map(c => c.customerID);
  const londonCustomerIDs = londonCustomers.map(c => c.customerID);

  const exclusiveNYCustomers = newyorkCustomerIDs.filter(id => !londonCustomerIDs.includes(id));

  const resultCustomers = await prisma.customer.findMany({
    where: { customerID: { in: exclusiveNYCustomers } },
    select: { customerID: true },
    orderBy: { customerID: 'asc' }
  });

  return resultCustomers.slice(0, 10);
}

async function problem8() {
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
  return employees.map(employee => ({
    sin: employee.sin,
    firstName: employee.firstName,
    lastName: employee.lastName,
    salary: employee.salary,
    branchName: employee.Branch_Branch_managerSINToEmployee[0]?.branchName || null
  }))
  .sort((a, b) => 
    a.branchName !== b.branchName ? 
    a.branchName === null ? 1 : 
    b.branchName === null ? -1 : 
    b.branchName.localeCompare(a.branchName) :
    a.firstName.localeCompare(b.firstName)
  )
  .slice(0, 10);
}

async function problem9() {
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
  return employees.map(employee => ({
    sin: employee.sin,
    firstName: employee.firstName,
    lastName: employee.lastName,
    salary: employee.salary,
    branchName: employee.Branch_Branch_managerSINToEmployee[0]?.branchName || null
  }))
  .sort((a, b) => 
    a.branchName !== b.branchName ? 
    a.branchName === null ? 1 : 
    b.branchName === null ? -1 : 
    b.branchName.localeCompare(a.branchName) :
    a.firstName.localeCompare(b.firstName)
  )
  .slice(0, 10);
}

async function problem10() {
  const helen = await prisma.customer.findMany({
    include: {
      Owns: {
        include: {
          Account: true
        }
      }
    },
    where: {
      firstName: 'Helen',
      lastName: 'Morgan'
    }
  });

  const helenBranches = helen.flatMap((customer) => customer.Owns.map(own => own.Account.branchNumber));

  const customers = await prisma.customer.findMany({
    include: {
      Owns: {
        include: {
          Account: true
        }
      }
    },
    where: {
      income : {
        gt: 5000
      }
    },
    orderBy: {
      income: 'desc'
    }
  });

  return customers
    .filter((customer) => {
      const customerBranches = new Set(customer.Owns.map((own) => own.Account.branchNumber));
      return helenBranches.every(branch => customerBranches.has(branch));
    })
    .map(
      ({ customerID, firstName, lastName, income }) => ({ customerID, firstName, lastName, income })
    );
}

async function problem11() {
  const branch = await prisma.branch.findMany({
    select: {
      branchNumber: true
    },
    where: {
      branchName : 'Berlin'
    }
  });

  const bN = branch.map(b => b.branchNumber)

  const employee = await prisma.employee.findMany({
    select: {
      sin: true,
      firstName: true,
      lastName: true,
      salary: true,
    },
    where: {
      branchNumber: {
        in: bN
      }
    },
    orderBy: {
      salary: 'asc'
    },
    take: 1
  });

  return employee;
}

async function problem14 () {
  const moscowEmployee = await prisma.employee.findMany({
    select: {
      salary: true,
    },
    where: {
      Branch_Employee_branchNumberToBranch: { branchName: "Moscow" },
    },
  });
  const totalSalary = moscowEmployee.reduce((total, employee) => total + employee.salary, 0);
  
  return [{"sum of exmployees salaries": totalSalary}];
}

async function problem15 () {
  const customers = await prisma.customer.findMany({
    include: {
      Owns: {
        include: {
          Account: {
            include: {
              Branch: true
            }
          }
        }
      }
    }
  });

  return customers
    .filter((customer) => {
      const branch = customer.Owns.map ((own) => own.Account.Branch.branchName);
      const uniqueBranch = new Set (branch);
      return uniqueBranch.size === 4;
    })
    .map((customer) => ({
      customerID: customer.customerID,
      firstName: customer.firstName,
      lastName: customer.lastName
    }))
    .sort ((a, b) => {
      if (a.lastName != b.lastName) return a.lastName.localeCompare(b.lastName);
      else return a.firstName .localeCompare(b.firstName);
    })
    .slice (0,10);
}

async function problem17() {
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

  return customers 
    .filter((customer) => customer.Owns.length >=3)
    .map((customer) => ({
      customerID: customer.customerID,
      firstName: customer.firstName,
      lastName: customer.lastName,
      income: customer.income,
      "average account balance": customer.Owns.reduce((acc, own, idx, arr) => acc + parseFloat(own.Account.balance) / arr.length, 0)
    }))
    .sort((a,b) => a.customerID - b.customerID);
}

async function problem18 () {
  const branch = await prisma.branch.findMany({
    where: {
      branchName: 'Berlin'
    }
  })

  const accounts = await prisma.account.findMany({
    include: {
      Transactions: true
    },
    where: {
      branchNumber: {
        in: branch.map((branch) => branch.branchNumber)
      }
    }
  });

  return accounts
    .filter((account) => account.Transactions.length >= 10)
    .map(account => ({
      accNumber: account.accNumber,
      balance: account.balance,
      "sum of transaction amounts": account.Transactions.reduce((acc, cur) => acc + parseFloat(cur.amount), 0)
    }))
    .sort((a,b) => a["sum of transaction amounts"] - b["sum of transaction amounts"])
    .slice(0,10);
}

const ProblemList = [
  { id: 1, query: problem1 }, { id: 2, query: problem2 }, { id: 3, query: problem3 }, { id: 4, query: problem4 }, { id: 5, query: problem5 }, 
  { id: 6, query: problem6 }, { id: 7, query: problem7 }, { id: 8, query: problem8 }, { id: 9, query: problem9 }, { id: 10, query: problem10 }, 
  { id: 11, query: problem11 }, { id: 14, query: problem14 }, { id: 15, query: problem15 }, { id: 17, query: problem17 },  { id: 18, query: problem18 }
];

function setupEndpoints() {
  for (const problem of ProblemList) {
    router.get(`/${problem.id}`, async (req, res) => {
      try {
        const result = await problem.query();
        if (result && result.length > 0) {
          res.json(result);
        } else if (result.length === 0) {
          res.json({ message: 'No results found.' });
        } else {
          res.status(500).json({ message: 'Query returned undefined.' });
        }
      } catch (error) {
        console.error(`Error executing problem ${problem.id}: ${error.message}`);
        res.status(500).json({ message: 'Error executing query', error: error.message });
      }
    });
  }
}
setupEndpoints();
module.exports = router;