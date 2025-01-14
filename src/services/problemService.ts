import prisma from '../prismaClient';
import Util from '../util';

export class ProblemService {
    constructor(private util: Util) {};

    async solve (id: number): Promise<any> {
        switch (id) {
            case 1:
                return this.problem1 ();
            case 2:
                return this.problem2 ();
            case 3:
                return this.problem3 ();
            case 4:
                return this.problem4 ();
            case 5:
                return this.problem5 ();
            case 6:
                return this.problem6 ();
            case 7:
                return this.problem7 ();
            case 8:
                return this.problem8 ();
            case 9:
                return this.problem9 ();
            case 10:
                return this.problem10 ();
            case 11:
                return this.problem11 ();
            case 14:
                return this.problem14 ();
            case 15:
                return this.problem15 ();
            case 17:
                return this.problem17 ();
            case 18:
                return this.problem18 ();
            default:
                throw new Error('invalid problem id');
        }
    }

    async problem1 () {
        return prisma.customer.findMany({
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
                {
                    income: 'desc',
                },
                {
                    lastName: 'asc',
                },
                {
                    firstName: 'asc',
                },
            ],
            take: 10,
        });
    }

    async problem2 () {
        const result = await prisma.branch.findMany({
            include: {
                Employee_Branch_managerSINToEmployee: {
                    select: {
                        salary: true,
                    },
                },
                Employee_Employee_branchNumberToBranch: {
                    select: {
                        sin: true,
                        salary: true,
                    },
                },
            },
            where: {
                branchName: {
                    in: ['London', 'Berlin'],
                },
            },
        });

        const formatted = result.flatMap(branch => {
            const managerSalary = branch.Employee_Branch_managerSINToEmployee?.salary;
            return branch.Employee_Employee_branchNumberToBranch.map(employee => ({
                sin: employee.sin,
                branchName: branch.branchName,
                salary: employee.salary,
                'Salary Diff': managerSalary! - employee?.salary!,
            }));
        });

        return formatted.sort((a, b) => {
            return b['Salary Diff'] - a['Salary Diff'];
        }).slice(0, 10);
    }

    async problem3 () {
        const [butlerIncome, result] = await Promise.all([
            prisma.customer.findMany({
                select: {
                    income: true,
                },
                where: {
                    lastName: 'Butler',
                },
            }),
            prisma.customer.findMany({
                select: {
                    firstName: true,
                    lastName: true,
                    income: true,
                },
                orderBy: [
                    {
                        lastName: "asc",
                    },
                    {
                        firstName: "asc",
                    },
                ],
            }),
        ]);

        return result.filter(item => {
            const income = item.income;
            return butlerIncome.every(refItem => (refItem.income || 0) < (income || 0) / 2)
        }).slice(0, 10);
    }

    async problem4 () {
        const includeClause = {
            Customer: {
                select: {
                    customerID: true,
                    income: true,
                },
            },
            Account: {
                select: {
                    accNumber: true,
                    branchNumber: true,
                    Branch: {
                        select: {
                            branchName: true,
                        },
                    },
                },
            },
        };

        const whereClause = {
            Customer: {
                income: {
                    gte: 80000,
                },
            },
        };

        const ownsDetailed = await prisma.owns.findMany({
            include: includeClause,
            where: whereClause,
        });

        const london = ownsDetailed.filter(owns => {
            return (owns.Account.Branch?.branchName == 'London');
        });

        const latveria = ownsDetailed.filter(owns => {
            return (owns.Account.Branch?.branchName == 'Latveria');
        });

        const customerIDs = this.util.getIntersection(london, latveria, "customerID").map(owns => {
            return owns.customerID;
        });

        return ownsDetailed.filter(owns => {
            return customerIDs.some((refCustomerID: number) => owns.customerID == refCustomerID)
        }).sort((a, b) => {
            return (a.customerID == b.customerID) ? (a.accNumber - b.accNumber) : (a.customerID - b.customerID);
        }).slice(0, 10).flatMap(owns => {
            return {
                customerID: owns.customerID,
                income: owns.Customer.income,
                accNumber: owns.accNumber,
                branchNumber: owns.Account.branchNumber,
            };
        });
    }

    async problem5 () {
        const ownsDetailed = await prisma.owns.findMany({
            include: {
                Customer: {
                    select: {
                        customerID: true,
                    },
                },
                Account: {
                    select: {
                        type: true,
                        accNumber: true,
                        balance: true,
                    },
                },
            },
            where: {
                Account: {
                    type: {
                        in: ['SAV', 'BUS'],
                    },
                },
            },
            orderBy: [
                {
                    Customer: {
                        customerID: "asc",
                    },
                },
                {
                    Account: {
                        type: "asc",
                    },
                },
                {
                    Account: {
                        accNumber: "asc",
                    }
                },
            ],
            take: 10,
        });

        return ownsDetailed.map(owns => {
            return {
                customerID: owns.customerID,
                type: owns.Account.type,
                accNumber: owns.accNumber,
                balance: owns.Account.balance,
            };
        });
    }

    async problem6 () {
        const target = await prisma.employee.findFirst({
            select: {
                sin: true,
            },
            where: {
                firstName: 'Phillip',
                lastName: 'Edwards',
            },
        })

        const accountWithBranch = await prisma.account.findMany({
            include: {
                Branch: {
                    select: {
                        branchName: true,
                    },
                },
            },
            where: {
                Branch: {
                    managerSIN: target?.sin,
                }
            },
            orderBy: {
                accNumber: "asc",
            },
        });

        return accountWithBranch.map(account => {
            return {
                branchName: account.Branch?.branchName,
                accNumber: account.accNumber,
                balance: account.balance,
            };
        }).filter(account => {
            return Number(account.balance) > 100000;
        }).slice(0, 10);
    }

    async problem7 () {
        const coOwnedAccounts = await prisma.owns.groupBy({
            by: ['accNumber'],
            _count: {
                customerID: true,
            },
            having: {
                customerID: {
                    _count: {
                        gte: 2,
                    }
                }
            },
        });

        const customerOwnsLondonAccounts = await prisma.owns.findMany({
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

        const accountsLondonCustomerCoOwns = await prisma.owns.findMany({
            select: {
                accNumber: true,
            },
            where: {
                customerID: {
                    in: customerOwnsLondonAccounts.map(customer => {
                        return customer.customerID;
                    }),
                },
                accNumber: {
                    in: coOwnedAccounts.map(account => {
                        return account.accNumber;
                    })
                }
            },
        });

        const customerCoOwnsAccountsWithLondonCustomer = await prisma.owns.findMany({
            select: {
                customerID: true,
            },
            where: {
                accNumber: {
                    in: accountsLondonCustomerCoOwns.map(account => {
                        return account.accNumber;
                    }),
                },
            },
        })

        return prisma.owns.findMany({
            select: {
                customerID: true,
            },
            distinct: "customerID",
            where: {
                customerID: {
                    notIn: [...customerOwnsLondonAccounts, ...customerCoOwnsAccountsWithLondonCustomer].map(
                        customer => {
                            return customer.customerID;
                        }
                    ),
                },
                Account: {
                    Branch: {
                        branchName: 'New York',
                    },
                },
            },
            orderBy: {
                customerID: "asc",
            },
            take: 10,
        });
    }

    async problem8 () {
        const employees = await prisma.employee.findMany({
            include: {
                Branch_Employee_branchNumberToBranch: {
                    select: {
                        branchName: true,
                    },
                },
            },
            where: {
                salary: {
                    gte: 50000,
                },
            },
        });

        const managers = await prisma.branch.findMany({
            select: {
                managerSIN: true,
                branchName: true,
            },
        });

        const managerMap = new Map(managers.map(m => {
            return [m.managerSIN, m.branchName];
        }));

        return employees.map(employee => {

            const isManager = managers.some(manager => {
                return manager.managerSIN == employee.sin;
            });

            const branchName = isManager ? managerMap.get(employee.sin) : null;

            return {
                sin: employee.sin,
                firstName: employee.firstName,
                lastName: employee.lastName,
                salary: employee.salary,
                branchName: branchName,
            };
        }).sort((a, b) => {
            return (a.branchName == b.branchName) ?
                this.util.asciiDifferenceFlexible(a.firstName, b.firstName)
            :
                this.util.asciiDifferenceFlexible(b.branchName, a.branchName);
        }).slice(0, 10);
    }

    async problem9 () {
        return this.problem8 ();
    }

    async problem10 () {
        const [ref, targetCustomerAccounts, targetCustomers] = await Promise.all([
            prisma.owns.findMany({
                select: {
                    Account: {
                        select: {
                            branchNumber: true,
                        },
                    },
                },
                where: {
                    Customer: {
                        firstName: 'Helen',
                        lastName: 'Morgan',
                    },
                },
            }),
            prisma.owns.findMany({
                select: {
                    customerID: true,
                    Account: {
                        select: {
                            branchNumber: true,
                        },
                    },
                },
                where: {
                    Customer: {
                        income: {
                            gt: 5000,
                        },
                    }
                },
            }),
            prisma.customer.findMany({
                select: {
                    customerID: true,
                    firstName: true,
                    lastName: true,
                    income: true,
                },
                where: {
                    income: {
                        gt: 5000
                    },
                },
                orderBy: {
                    income: "desc",
                },
            })
        ]);

        const refBranches = ref.map(owns => {
            return owns.Account.branchNumber;
        });

        const filtered =  targetCustomers.filter(customer => {
            const branches = targetCustomerAccounts.filter(owns => {
                return owns.customerID == customer.customerID;
            }).map(owns => owns.Account.branchNumber);
            return refBranches.every(refBranch => {
                return branches.includes(refBranch);
            });
        });

        return filtered.map(customer => {
            return {
                customerID: customer.customerID,
                firstName: customer.firstName,
                lastName: customer.lastName,
                income: customer.income,
            };
        }).slice(0, 10);
    }

    async problem11 () {
        const minSalary = (await prisma.employee.aggregate({
            _min: {
                salary: true,
            },
        }))._min.salary;

        return prisma.employee.findMany({
            select: {
                sin: true,
                firstName: true,
                lastName: true,
                salary: true,
            },
            where: {
                salary: minSalary,
            },
        });
    };

    async problem14 () {
        const result = await prisma.employee.aggregate({
            _sum: {
                salary: true,
            },
            where: {
                Branch_Employee_branchNumberToBranch: {
                    branchName: 'Moscow',
                },
            },
        });

        return [
            {
                "sum of employees salaries": result._sum.salary,
            }
        ];
    };

    async problem15 () {
        const [owns, customers] = await Promise.all([
            prisma.owns.findMany({
                select: {
                    customerID: true,
                    Account: {
                        select: {
                            branchNumber: true
                        },
                    },
                },
            }),
            prisma.customer.findMany({
                select: {
                    customerID: true,
                    firstName: true,
                    lastName: true,
                },
                orderBy: [
                    {
                        lastName: "asc"
                    },
                    {
                        firstName: "asc"
                    },
                ],
            }),
        ]);

        return customers.filter(customer => {
            const branchNumbers = new Set(
                owns.filter(own => {
                    return own.customerID == customer.customerID;
                }).map(own => {
                    return own.Account.branchNumber;
                })
            );
            return branchNumbers.size === 4;
        }).slice(0, 10);
    };

    async problem17 () {
        const whereClause = {
            Customer: {
                lastName: {
                    startsWith: 'S',
                    contains: 'e'
                },
            },
        };

        const [targetCustomers, customersOwnAccounts] = await Promise.all([
            prisma.owns.groupBy({
                by: ['customerID'],
                _count: {
                    accNumber: true,
                },
                where: whereClause,
                orderBy: {
                    customerID: "asc",
                },
                having: {
                    accNumber: {
                        _count: {
                            gte: 3
                        },
                    },
                },
            }),
            prisma.owns.findMany({
                select: {
                    customerID: true,
                    Customer: {
                        select: {
                            firstName: true,
                            lastName: true,
                            income: true,
                        }
                    },
                    Account: {
                        select: {
                            balance: true,
                        },
                    },
                },
                where: whereClause,
            }),
        ]);

        const customersOwnAccountsMap = new Map(customersOwnAccounts.map(owns => {
            return [
                owns.customerID,
                {
                    firstName: owns.Customer.firstName,
                    lastName: owns.Customer.lastName,
                    income: owns.Customer.income,
                    owns: owns.Account.balance,
                }
            ];
        }));

        return targetCustomers.map(targetCustomer => {
            const avgBalance = customersOwnAccounts.filter(owns => {
                return owns.customerID == targetCustomer.customerID;
            }).map(owns => {
                return owns.Account.balance;
            }).reduce((acc, balance) => {
                return acc + Number(balance!);
            }, 0) / targetCustomer._count.accNumber;

            const customerInfo = customersOwnAccountsMap.get(targetCustomer.customerID);
            return {
                customerID: targetCustomer.customerID,
                firstName: customerInfo?.firstName,
                lastName: customerInfo?.lastName,
                income: customerInfo?.income,
                "average account balance": avgBalance,
            };
        }).slice(0, 10);
    };

    async problem18 () {

        const transactions = await prisma.transactions.findMany({
            select: {
                amount: true,
                Account: {
                    select: {
                        accNumber: true,
                        balance: true,
                        Branch: {
                            select: {
                                branchNumber: true
                            },
                        },
                    },
                },
            },
            where: {
                Account: {
                    Branch: {
                        branchName: 'Berlin',
                    },
                },
            },
        });

        const transactionMap = new Map<number, {
            accNumber: number;
            balance: string | null;
            amount: number | null;
            count: number;
        }>();

        for (const transaction of transactions) {
            const accNumber = transaction.Account.accNumber;

            if (!transactionMap.has(accNumber)) {
                transactionMap.set(accNumber, {
                    accNumber,
                    balance: transaction.Account.balance,
                    amount: Number(transaction.amount),
                    count: 1,
                });
            } else {
                const current = transactionMap.get(accNumber)!; // Non-null assertion
                transactionMap.set(accNumber, {
                    ...current,
                    amount: current.amount! + Number(transaction.amount!),
                    count: current.count + 1,
                });
            }
        }

        return (Array.from(transactionMap.values()).filter(account => {
            return account.count >= 10;
        }).sort((a, b) => {
            return a.amount! - b.amount!;
        })).map(account => {
            return {
                accNumber: account.accNumber,
                balance: String(account.balance),
                'sum of transaction amounts': account.amount,
            };
        }).slice(0, 10);
    }
}

export default ProblemService;
