const express = require('express');
const router = express.Router();

const {PrismaClient} = require('@prisma/client');
const prisma = new PrismaClient();

router.post('/join', async (req, res) => {
    try {
        const input = req.body;
        const user = await prisma.employee.create({
            data: {
                sin: input["sin"],
                firstName: input["firstName"],
                lastName: input["lastName"],
                salary: input["salary"],
                branchNumber: input["branchNumber"],
            }
        })
        res.status(200).json({success: true, msg: `이 팀은 미친듯이 일하는 일꾼들로 이루어진 광전사 설탕 노움 조합이다.
        분위기에 적응하기는 쉽지 않지만 아주 화력이 좋은 강력한 조합인거 같다.`});
    } catch(e) {
        res.status(500).json({error: e});
    }
});

router.post('/leave', async (req, res) => {
    try {
        const input = req.body;
        const deleteEmployee = await prisma.employee.delete({
            where: {
                sin: input["sin"],
            }
        })
        res.status(200).json({success: true, msg: `안녕히 계세요 여러분!
        전 이 세상의 모든 굴레와 속박을 벗어 던지고 제 행복을 찾아 떠납니다!
        여러분도 행복하세요~~!`});
    } catch(e) {
        res.status(500).json({error: e});
    }
});

module.exports = router;