import { Router } from 'express';
import EmployeeService from '../services/employeeService';

const employeeRouter = Router();

const employeeService = new EmployeeService();

employeeRouter.post('/join', async (req, res) => {
    try {
        const { sin, firstName, lastName, salary, branchNumber } = req.body;
        await employeeService.join({ sin, firstName, lastName, salary, branchNumber });
        res.send('이 팀은 미친듯이 일하는 일꾼들로 이루어진 광전사 설탕 노움 조합이다.\n' +
            '분위기에 적응하기는 쉽지 않지만 아주 화력이 좋은 강력한 조합인거 같다.')
    } catch(e) {
        console.error(e);
        res.status(500).send({ message: 'An error occurred.'});
    }
});

employeeRouter.delete('/leave', async (req, res) => {
    try {
        const { sin } = req.body;
        await employeeService.leave({ sin });
        res.send('안녕히 계세요 여러분!\n' +
            '전 이 세상의 모든 굴레와 속박을 벗어 던지고 제 행복을 찾아 떠납니다!\n' +
            '여러분도 행복하세요~~!');
    } catch (e) {
        console.error(e);
        res.status(500).send({ message: 'An error occurred.'});
    }
});

export default employeeRouter;
