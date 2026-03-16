import unittest
from unittest.mock import AsyncMock, patch
import os

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

os.environ["DEBUG"] = "false"

from app.database import Base
from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.redemption import RedemptionService


class RedemptionServiceTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        self.service = RedemptionService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_mark_distributed_preserves_warranty_fields(self):
        async with self.session_factory() as session:
            code = RedemptionCode(
                code="TEST-DIST-001",
                status="unused",
                has_warranty=True,
                warranty_days=45,
            )
            session.add(code)
            await session.commit()

            result = await self.service.mark_codes_distributed(
                ["TEST-DIST-001"], session
            )

            self.assertTrue(result["success"])

            refreshed = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "TEST-DIST-001")
            )
            self.assertEqual(refreshed.status, "distributed")
            self.assertTrue(refreshed.has_warranty)
            self.assertEqual(refreshed.warranty_days, 45)

    async def test_mark_distributed_can_update_edit_fields_in_one_call(self):
        async with self.session_factory() as session:
            code = RedemptionCode(
                code="TEST-DIST-EDIT-001",
                status="unused",
                has_warranty=False,
                warranty_days=30,
                remark="old",
            )
            session.add(code)
            await session.commit()

            result = await self.service.mark_codes_distributed(
                ["TEST-DIST-EDIT-001"],
                session,
                remark="new remark",
                has_warranty=True,
                warranty_days=60,
            )

            self.assertTrue(result["success"])

            refreshed = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "TEST-DIST-EDIT-001")
            )
            self.assertEqual(refreshed.status, "distributed")
            self.assertTrue(refreshed.has_warranty)
            self.assertEqual(refreshed.warranty_days, 60)
            self.assertEqual(refreshed.remark, "new remark")

    async def test_mark_distributed_rejects_used_codes(self):
        async with self.session_factory() as session:
            code = RedemptionCode(
                code="TEST-USED-001",
                status="used",
                used_by_email="used@example.com",
            )
            session.add(code)
            await session.commit()

            result = await self.service.mark_codes_distributed(
                ["TEST-USED-001"], session
            )

            self.assertFalse(result["success"])

            refreshed = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "TEST-USED-001")
            )
            self.assertEqual(refreshed.status, "used")

    async def test_unused_filter_includes_distributed_codes(self):
        async with self.session_factory() as session:
            session.add_all(
                [
                    RedemptionCode(code="TEST-FILTER-UNUSED", status="unused"),
                    RedemptionCode(code="TEST-FILTER-DIST", status="distributed"),
                    RedemptionCode(code="TEST-FILTER-USED", status="used"),
                ]
            )
            await session.commit()

            result = await self.service.get_all_codes(
                session,
                page=1,
                per_page=20,
                status="unused",
            )

            self.assertTrue(result["success"])
            returned_codes = {item["code"] for item in result["codes"]}
            self.assertEqual(
                returned_codes,
                {"TEST-FILTER-UNUSED", "TEST-FILTER-DIST"},
            )

    async def test_withdraw_record_restores_distributed_status(self):
        async with self.session_factory() as session:
            team = Team(
                email="team@example.com",
                access_token_encrypted="token",
                account_id="acct-1",
                team_name="Team One",
            )
            code = RedemptionCode(
                code="TEST-WITHDRAW-001",
                status="used",
                used_by_email="user@example.com",
                has_warranty=True,
            )
            session.add_all([team, code])
            await session.flush()

            code.used_team_id = team.id
            record = RedemptionRecord(
                email="user@example.com",
                code=code.code,
                team_id=team.id,
                account_id="acct-1",
                previous_code_status="distributed",
            )
            session.add(record)
            await session.commit()

            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(return_value={"success": True}),
            ):
                result = await self.service.withdraw_record(record.id, session)

            self.assertTrue(result["success"])

            refreshed_code = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "TEST-WITHDRAW-001")
            )
            self.assertEqual(refreshed_code.status, "distributed")
            self.assertIsNone(refreshed_code.used_by_email)
            self.assertIsNone(refreshed_code.used_team_id)
            self.assertIsNone(refreshed_code.used_at)
            self.assertIsNone(refreshed_code.warranty_expires_at)

            refreshed_record = await session.scalar(
                select(RedemptionRecord).where(RedemptionRecord.id == record.id)
            )
            self.assertIsNone(refreshed_record)

    async def test_withdraw_record_restores_previous_warranty_active_status(self):
        async with self.session_factory() as session:
            team = Team(
                email="team2@example.com",
                access_token_encrypted="token",
                account_id="acct-2",
                team_name="Team Two",
            )
            code = RedemptionCode(
                code="TEST-WITHDRAW-WARRANTY",
                status="used",
                used_by_email="user2@example.com",
                has_warranty=True,
            )
            session.add_all([team, code])
            await session.flush()

            code.used_team_id = team.id
            record = RedemptionRecord(
                email="user2@example.com",
                code=code.code,
                team_id=team.id,
                account_id="acct-2",
                is_warranty_redemption=True,
                previous_code_status="warranty_active",
            )
            session.add(record)
            await session.commit()

            with patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(return_value={"success": True}),
            ):
                result = await self.service.withdraw_record(record.id, session)

            self.assertTrue(result["success"])

            refreshed_code = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "TEST-WITHDRAW-WARRANTY")
            )
            self.assertEqual(refreshed_code.status, "warranty_active")


if __name__ == "__main__":
    unittest.main()
