const express = require('express');
const router = express.Router();

const {PrismaClient} = require('@prisma/client');
const { max, min, set, sum } = require('lodash');
const { notEqual } = require('assert');
const prisma = new PrismaClient()

router.get('/', (req, res) => {
    try {
        send('Problems router!');
    } catch (e) {
        res.status(500).json({error: e});
    }
})

router.get('/1', async (req, res) => {
    try {
        const result = await prisma.customer.findMany({
            where: {
                AND: [
                    {
                        income: {
                            gte: 50000
                        },
                    },
                    {
                        income: {
                            lte: 60000
                        },
                    }
                ]
            },
            select: {
                firstName: true,
                lastName: true,
                income: true
            },
            take: 10,
            orderBy: [
                    {
                        income: 'desc'
                    },
                    {
                        lastName: 'asc'
                    },
                    {
                        firstName: 'asc'
                    },
            ]
        });
        const answer = result;
        res.status(200).json(answer);
    } catch (e) {
        res.status(500).json({error: e});
    }
})

router.get('/2', async (req, res) => {
    try {
        const result = await prisma.employee.findMany({
            where: {
                OR: [
                    {
                        Branch_Employee_branchNumberToBranch: {
                            branchName: "London"
                        },
                    },
                    {
                        Branch_Employee_branchNumberToBranch: {
                            branchName: "Berlin"
                        },
                    }
                ],
            },
            select: {
                sin: true,
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
                salary: true,
            }
        });
        const answer = [];
        result.forEach((obj) => {
            answer.push({
                "sin": obj.sin,
                "branchName": obj.Branch_Employee_branchNumberToBranch.branchName,
                "salary": obj.salary,
                "Salary Diff": obj.Branch_Employee_branchNumberToBranch.Employee_Branch_managerSINToEmployee.salary - obj.salary,
            });
        })
        answer.sort((a,b) => -(a["Salary Diff"] - b["Salary Diff"]));
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0, 10));
    } catch (e) {
        res.status(500).json({error: e});
    }
})

router.get('/3', async (req, res) => {
    try {
        const incomesOfButlers = await prisma.customer.findMany({
            where: {
                lastName: {
                    equals: "Butler",
                }
            },
            select: {
                income: true
            },
        });

        let maxIncome = -1;
        incomesOfButlers.forEach((obj) => {
            maxIncome = Math.max(maxIncome, obj.income);
        })

        const result = await prisma.customer.findMany({
            where: {
                income: {
                    gte: 2 * maxIncome
                }
            },
            select: {
                firstName: true,
                lastName: true,
                income: true
            },
            orderBy: [
                {
                    lastName: 'asc',
                },
                {
                    firstName: 'asc',
                },
            ],
            take: 10
        })
        const answer = result;
        res.status(200).json(answer);
    } catch (e) {
        res.status(500).json({error: e});
    }
})

router.get('/4', async (req, res) => {
    try {
        result = await prisma.customer.findMany({
            where: {
                income: {
                    gt: 80000,
                },
                AND: [
                    {
                        Owns: {
                            some: {
                                Account: {
                                    Branch: {
                                        branchName: "London",
                                    }
                                }
                            }
                        },
                    },
                    {
                        Owns: {
                            some: {
                                Account: {
                                    Branch: {
                                        branchName: "Latveria",
                                    }
                                }
                            }
                        },
                    }
                ]
            },
            select: {
                customerID: true,
                income: true,
                Owns: {
                    select: {
                        accNumber: true,
                        Account: {
                            select: {
                                branchNumber: true,
                            }
                        }
                    }
                },
                
            }
        })
        const answer = [];
        result.forEach((customer) => {
            customer.Owns.forEach((account) => {
                answer.push({
                    "customerID": customer.customerID,
                    "income": customer.income,
                    "accNumber": account.accNumber,
                    "branchNumber": account.Account.branchNumber
                })
            })
        })
        answer.sort((a,b) => a.customerID - b.customerID || a.accNumber - b.accNumber);
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0,10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/5', async (req, res) => {
    try {
        result = await prisma.customer.findMany({
            where: {
                Owns: {
                    some: {
                        Account: {
                            OR: [
                                {
                                    type: "BUS",
                                },
                                {
                                    type: "SAV",
                                }
                            ]
                        }
                    }
                }
            },
            select: {
                customerID: true,
                Owns: {
                    select: {
                        Account: {
                            select: {
                                type: true,
                                accNumber: true,
                                balance: true,
                            }
                        }
                    }
                }
            }
        })
        const answer = [];
        result.forEach((customer) => {
            customer.Owns.forEach((account) => {
                if (account.Account.type === "SAV" || account.Account.type == "BUS"){
                    answer.push({
                        "customerID": customer.customerID,
                        "type": account.Account.type,
                        "accNumber": account.Account.accNumber,
                        "balance": account.Account.balance,
                    })
                }
            })
        })
        answer.sort((a,b) => a.customerID - b.customerID || a.type.localeCompare(b.type) || a.accNumber - b.accNumber);
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0,10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/6', async (req, res) => {
    try {
        const PhillipEdwards = await prisma.employee.findFirst({
            where: {
                firstName: "Phillip",
                lastName: "Edwards",
            },
        })
        const result = await prisma.account.findMany({
            where: {
                Branch: {
                    managerSIN: PhillipEdwards.sin
                }
            },
            select: {
                Branch: {
                    select: {
                        branchName: true,
                    }
                },
                accNumber: true,
                balance: true,
            },
        })
        const answer = [];
        result.forEach((obj) => {
            if(Number(obj.balance) > 100000){
                answer.push({
                    "branchName": obj.Branch.branchName,
                    "accNumber": obj.accNumber,
                    "balance": obj.balance,
                })
            }
        })
        answer.sort((a,b) => a.accNumber - b.accNumber);
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0,10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/7', async (req, res) => {
    try {
        const result = await prisma.customer.findMany({
            where: {
                AND: [
                    {
                        Owns: {
                            some: {
                                Account: {
                                    Branch: {
                                        branchName: "New York",
                                    }
                                }
                            }
                        },
                    },
                    {
                        Owns: {
                            every: {
                                Account: {
                                    Branch: {
                                        NOT: {
                                            branchName: "London"
                                        },
                                    }
                                }
                            }
                        },
                    },
                    {
                        NOT: {
                            Owns: {
                                some: {
                                    Account: {
                                        Owns: {
                                            some: {
                                                Customer: {
                                                    Owns: {
                                                        some: {
                                                            Account: {
                                                                Branch: {
                                                                    branchName: "London",
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ]      
            },
            select: {
                customerID: true
            },
            orderBy: [
                {
                    customerID: 'asc',
                },
            ],
            take: 10
        })
        const answer = result;
        res.status(200).json(answer);
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/8', async (req, res) => {
    try {
        const objectManagerSINs = await prisma.branch.findMany({
            select: {
                managerSIN: true,
            }
        });
        const managerSINs = [];
        objectManagerSINs.forEach((obj) => {
            managerSINs.push(obj.managerSIN);
        });
        const result = await prisma.employee.findMany({
            where: {
                salary: {
                    gt: 50000
                },
            },
            select: {
                sin: true,
                firstName: true,
                lastName: true,
                salary: true,
                Branch_Employee_branchNumberToBranch: {
                    select: {
                        branchName: true,
                    }
                }
            }
        })
        const answer = [];
        result.forEach((employee) => {
            let isManager = false;
            for (let i=0; i<managerSINs.length; i++){
                if (managerSINs[i] === employee.sin) isManager = true;
            }
            if (isManager) {
                answer.push({
                    "sin": employee.sin,
                    "firstName": employee.firstName,
                    "lastName": employee.lastName,
                    "salary": employee.salary,
                    "branchName": employee.Branch_Employee_branchNumberToBranch.branchName, 
                })
            } else {
                answer.push({
                    "sin": employee.sin,
                    "firstName": employee.firstName,
                    "lastName": employee.lastName,
                    "salary": employee.salary,
                    "branchName": "null", 
                })
            }
        })
        answer.sort((a,b) => a.branchName.localeCompare(b.branchName) * (-1) || a.firstName.localeCompare(b.findFirst))
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0,10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/9', async (req, res) => {
    try {
        const objectManagerSINs = await prisma.branch.findMany({
            select: {
                managerSIN: true,
            }
        });
        const managerSINs = [];
        objectManagerSINs.forEach((obj) => {
            managerSINs.push(obj.managerSIN);
        });
        const result = await prisma.employee.findMany({
            where: {
                salary: {
                    gt: 50000
                },
            },
            select: {
                sin: true,
                firstName: true,
                lastName: true,
                salary: true,
                Branch_Employee_branchNumberToBranch: {
                    select: {
                        branchName: true,
                    }
                }
            }
        })
        const answer = [];
        result.forEach((employee) => {
            let isManager = false;
            for (let i=0; i<managerSINs.length; i++){
                if (managerSINs[i] === employee.sin) isManager = true;
            }
            if (isManager) {
                answer.push({
                    "sin": employee.sin,
                    "firstName": employee.firstName,
                    "lastName": employee.lastName,
                    "salary": employee.salary,
                    "branchName": employee.Branch_Employee_branchNumberToBranch.branchName, 
                })
            } else {
                answer.push({
                    "sin": employee.sin,
                    "firstName": employee.firstName,
                    "lastName": employee.lastName,
                    "salary": employee.salary,
                    "branchName": "null", 
                })
            }
        })
        answer.sort((a,b) => a.branchName.localeCompare(b.branchName) * (-1) || a.firstName.localeCompare(b.findFirst))
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0,10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/10', async (req, res) => {
    try {
        const objectBranchNumbersHelenOwns = await prisma.branch.findMany({
            where: {
                Account: {
                    some: {
                        Owns: {
                            some: {
                                Customer: {
                                    AND: [
                                        {
                                            firstName: "Helen",
                                        },
                                        {
                                            lastName: "Morgan",
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            select: {
                branchNumber: true,
            }
        })
        const listBranchNumbersHelenOwns = [];
        objectBranchNumbersHelenOwns.forEach((obj) => {
            listBranchNumbersHelenOwns.push(obj.branchNumber);
        })
        const result = await prisma.customer.findMany({
            where: {
                income: {
                    gt: 5000
                },
            },
            select: {
                customerID: true,
                firstName: true,
                lastName: true,
                income: true,
                Owns: {
                    select: {
                        Account: {
                            select: {
                                Branch: {
                                    select: {
                                        branchNumber: true,
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        const answer = [];
        result.forEach((obj) =>  {
            let doesItContainHelen = true;
            for (let i=0; i < listBranchNumbersHelenOwns.length; i++){
                let count = 0;
                obj.Owns.forEach((subobj) => {
                    if (subobj.Account.Branch.branchNumber === listBranchNumbersHelenOwns[i]){
                        count++;
                    }
                })
                if (count === 0) {
                    doesItContainHelen = false;
                }
            }
            if (doesItContainHelen) {
                answer.push({
                    "customerID": obj.customerID,
                    "firstName": obj.firstName,
                    "lastName": obj.lastName,
                    "income": obj.income,
                })
            }
        });
        answer.sort((a,b) => -(a.income - b.income));
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0,10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/11', async (req, res) => {
    try {
        const result = await prisma.employee.findMany({
            where: {
                Branch_Employee_branchNumberToBranch: {
                    branchName: "Berlin",
                }
            },
            select: {
                sin: true,
                firstName: true,
                lastName: true,
                salary: true,
            }
        });
        let minSalary = Infinity;
        result.forEach((obj) => {
            minSalary = Math.min(minSalary, obj.salary);
        });
        const answer = [];
        result.forEach((obj) => {
            if(obj.salary <= minSalary) {
                answer.push({
                    "sin": obj.sin,
                    "firstName": obj.firstName,
                    "lastName": obj.lastName,
                    "salary": obj.salary,
                });
            }
        });
        res.status(200).json(answer);
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/14', async (req, res) => {
    try {
        const result = await prisma.employee.aggregate({
            where: {
                Branch_Employee_branchNumberToBranch: {
                    branchName: "Moscow",
                }
            },
            _sum: {
                salary: true
            }
        });
        const answer = result._sum.salary;
        res.status(200).json({"sum of employees salaries": answer});
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/15', async (req, res) => {
    try {
        const result = await prisma.customer.findMany({
            select: {
                customerID: true,
                firstName: true,
                lastName: true,
                Owns: {
                    select: {
                        Account: {
                            select: {
                                branchNumber: true,
                            }
                        }
                    }
                }
            }
        })
        const answer = [];
        result.forEach((obj) => {
            const setOfBranchNumbers = new Set();
            obj.Owns.forEach((subobj) => {
                setOfBranchNumbers.add(subobj.Account.branchNumber);
            })
            if (setOfBranchNumbers.size === 4) {
                answer.push({
                    "customerID": obj.customerID,
                    "firstName": obj.firstName,
                    "lastName": obj.lastName,
                });
            }
        });
        answer.sort((a,b) => a.lastName.localeCompare(b.lastName));
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0,10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/17', async (req, res) => {
    try {
        const result = await prisma.customer.findMany({
            where: {
                AND: [
                    {
                        lastName: {
                            startsWith: "S",
                        },
                    },
                    {
                        lastName: {
                            contains: "e",
                        }
                    }
                ]
            },
            select:{
                customerID: true,
                firstName: true,
                lastName: true,
                income: true,
                Owns: {
                    select: {
                        Account: {
                            select: {
                                balance: true,
                            }
                        }
                    }
                }
            }
        });
        const answer = [];
        result.forEach((obj) => {
            if (obj.Owns.length >= 3) {
                let sum = 0.0;
                obj.Owns.forEach((subobj) => {
                    sum = sum + Number(subobj.Account.balance);
                });
                answer.push({
                    "customerID": obj.customerID,
                    "firstName": obj.firstName,
                    "lastName": obj.lastName,
                    "average account balance": sum / obj.Owns.length,
                });
            }
        });
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0, 10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.get('/18', async (req, res) => {
    try {
        const result = await prisma.account.findMany({
            where: {
                Branch: {
                    branchName: "Berlin",
                }
            },
            select: {
                accNumber: true,
                balance: true,
                Transactions: {
                    select: {
                        amount: true,
                    }
                },
            }
        });
        const answer = [];
        result.forEach((obj) => {
            if(obj.Transactions.length >= 10) {
                let sumOfTransactionAmounts = 0;
                obj.Transactions.forEach((transaction) => {
                    sumOfTransactionAmounts = sumOfTransactionAmounts + (transaction.amount);
                });
                answer.push({
                    "accNumber": obj.accNumber,
                    "balance": obj.balance,
                    "sum of transaction amounts": sumOfTransactionAmounts,
                });
            }
        });
        answer.sort((a,b) => a["sum of transaction amounts"] - b["sum of transaction amounts"]);
        res.status(200).json(answer.length < 10 ? answer : answer.slice(0, 10));
    } catch(e) {
        res.status(500).json({error: e});
    }
})

module.exports = router;