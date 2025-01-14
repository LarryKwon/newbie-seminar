import prisma from '../prismaClient';

export class AccountService {
    async deposit ({ accNumber, customerID, amount, }: {
        accNumber: number, customerID: number, amount: number,
    }) {
        return prisma.$transaction(async (prisma) => {
            const account = await prisma.owns.findUnique({
                select: {
                    Account: {
                        select: {
                            balance: true,
                        },
                    },
                },
                where: {
                    customerID_accNumber: {
                        customerID,
                        accNumber,
                    },
                },
            });

            if (!account) {
                throw new Error('Forbidden');
            }

            const updatedBalance = (await prisma.account.update({
                where: { accNumber },
                data: { balance: String(Number(account.Account.balance) + amount) }
            })).balance;

            const nextTransNumber = ((await prisma.transactions.findFirst({
                where: { accNumber },
                orderBy: { transNumber: 'desc' },
                select: { transNumber: true },
            }))?.transNumber || 0) + 1;

            const transactionLog = await prisma.transactions.create({
                data: {
                    accNumber,
                    transNumber: nextTransNumber,
                    amount: String(amount),
                },
            });

            return updatedBalance;
        }, { isolationLevel: 'Serializable' }, );
    }

    async withdraw ({ accNumber, customerID, amount, }: {
        accNumber: number, customerID: number, amount: number,
    }) {
        return prisma.$transaction(async (prisma) => {
            const account = await prisma.owns.findUnique({
                select: {
                    Account: {
                        select: {
                            balance: true,
                        },
                    },
                },
                where: {
                    customerID_accNumber: {
                        customerID,
                        accNumber,
                    },
                },
            });

            const currentBalance = Number(account?.Account.balance);

            if (!account) {
                throw new Error('Forbidden');
            } else if (currentBalance < amount) {
                throw new Error('Insufficient balance');
            }

            const updatedBalance = (await prisma.account.update({
                where: { accNumber },
                data: { balance: String(currentBalance - amount) }
            })).balance;

            const nextTransNumber = ((await prisma.transactions.findFirst({
                where: { accNumber },
                orderBy: { transNumber: 'desc' },
                select: { transNumber: true },
            }))?.transNumber || 0) + 1;

            const transactionLog = await prisma.transactions.create({
                data: {
                    accNumber,
                    transNumber: nextTransNumber,
                    amount: '-'+String(amount),
                },
            });

            return updatedBalance;
        }, { isolationLevel: 'Serializable' }, );
    };
}
