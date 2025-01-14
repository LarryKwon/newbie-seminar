import prisma from '../prismaClient';

export class EmployeeService {
    async join ({ sin, firstName, lastName, salary, branchNumber }: {
        sin: number, firstName: string, lastName: string, salary: number, branchNumber: number,
    }) {
        await prisma.employee.create({
            data: { sin, firstName, lastName, salary, branchNumber, },
        });
    }

    async leave ({ sin, }: { sin: number, }) {
        await prisma.employee.delete({
            where: { sin },
        });
    }
}

export default EmployeeService;
