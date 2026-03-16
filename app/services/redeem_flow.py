"""
兑换流程服务 (Redeem Flow Service)
协调兑换码验证, Team 选择和加入 Team 的完整流程
"""

import logging
import asyncio
import traceback
from collections import defaultdict
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import AsyncSessionLocal

from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.redemption import RedemptionService
from app.services.team import TeamService
from app.services.warranty import warranty_service
from app.services.notification import notification_service
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)

# 全局兑换锁: 针对 code 进行加锁，防止同一个码并发请求
_code_locks = defaultdict(asyncio.Lock)
# 全局 Team 锁: 针对 Team 进行加锁，防止并发拉人导致的人数状态不同步
_team_locks = defaultdict(asyncio.Lock)


class RedeemFlowService:
    """兑换流程场景服务类"""

    def __init__(self):
        """初始化兑换流程服务"""
        from app.services.chatgpt import chatgpt_service

        self.redemption_service = RedemptionService()
        self.warranty_service = warranty_service
        self.team_service = TeamService()
        self.chatgpt_service = chatgpt_service

    async def verify_code_and_get_teams(
        self, code: str, db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        验证兑换码并返回可用 Team 列表
        针对 aiosqlite 进行优化，避免 greenlet_spawn 报错
        """
        try:
            # 1. 验证兑换码
            validate_result = await self.redemption_service.validate_code(
                code, db_session
            )

            if not validate_result["success"]:
                return {
                    "success": False,
                    "valid": False,
                    "reason": None,
                    "teams": [],
                    "error": validate_result["error"],
                }

            # 如果是已经标记为过期了
            if (
                not validate_result["valid"]
                and validate_result.get("reason")
                == "兑换码已过期 (超过首次兑换截止时间)"
            ):
                try:
                    await db_session.commit()
                except:
                    pass

            if not validate_result["valid"]:
                return {
                    "success": True,
                    "valid": False,
                    "reason": validate_result["reason"],
                    "teams": [],
                    "error": None,
                }

            # 2. 获取可用 Team 列表
            teams_result = await self.team_service.get_available_teams(db_session)

            if not teams_result["success"]:
                return {
                    "success": False,
                    "valid": True,
                    "reason": "兑换码有效",
                    "teams": [],
                    "error": teams_result["error"],
                }

            logger.info(
                f"验证兑换码成功: {code}, 可用 Team 数量: {len(teams_result['teams'])}"
            )

            return {
                "success": True,
                "valid": True,
                "reason": "兑换码有效",
                "teams": teams_result["teams"],
                "error": None,
            }

        except Exception as e:
            logger.error(f"验证兑换码并获取 Team 列表失败: {e}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "valid": False,
                "reason": None,
                "teams": [],
                "error": f"验证失败: {str(e)}",
            }

    async def select_team_auto(
        self, db_session: AsyncSession, exclude_team_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        自动选择一个可用的 Team
        """
        try:
            # 查找所有 active 且未满的 Team
            stmt = select(Team).where(
                Team.status == "active", Team.current_members < Team.max_members
            )

            if exclude_team_ids:
                stmt = stmt.where(Team.id.not_in(exclude_team_ids))

            # 优先选择人数最少的 Team (负载均衡)
            stmt = stmt.order_by(Team.current_members.asc(), Team.created_at.desc())

            result = await db_session.execute(stmt)
            team = result.scalars().first()

            if not team:
                reason = "没有可用的 Team"
                if exclude_team_ids:
                    reason = "您已加入所有可用 Team"
                return {"success": False, "team_id": None, "error": reason}

            logger.info(f"自动选择 Team: {team.id}")

            return {"success": True, "team_id": team.id, "error": None}

        except Exception as e:
            logger.error(f"自动选择 Team 失败: {e}")
            return {
                "success": False,
                "team_id": None,
                "error": f"自动选择 Team 失败: {str(e)}",
            }

    async def redeem_and_join_team(
        self, email: str, code: str, team_id: Optional[int], db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        完整的兑换流程 (带事务和并发控制)

        注意: db_session 参数保留是为了兼容调用方的依赖注入，
        函数内部每次重试都使用独立的 AsyncSessionLocal() session，
        不直接使用传入的 db_session 参与任何事务边界。
        """
        last_error = "未知错误"
        max_retries = 3
        current_target_team_id = team_id
        core_success = False
        success_result = None
        team_id_final = None
        excluded_team_ids: List[int] = []  # 方案二：记录本次请求中已失败的 team，避免重试时重复选中

        # 针对 code 加锁，防止同一个码并发进入兑换
        async with _code_locks[code]:
            for attempt in range(max_retries):
                logger.info(
                    f"兑换尝试 {attempt + 1}/{max_retries} (Code: {code}, Email: {email})"
                )

                # 每次重试都使用独立的 session，彻底隔离上一轮的 session 状态
                async with AsyncSessionLocal() as retry_db:
                    try:
                        # 确定目标 Team (初选)
                        team_id_final = current_target_team_id
                        if not team_id_final:
                            select_res = await self.select_team_auto(retry_db, exclude_team_ids=excluded_team_ids or None)
                            if not select_res["success"]:
                                return {"success": False, "error": select_res["error"]}
                            team_id_final = select_res["team_id"]
                            current_target_team_id = team_id_final

                        # 使用 Team 锁序列化对该账户的操作，防止并发冲突
                        async with _team_locks[team_id_final]:
                            logger.info(
                                f"锁定 Team {team_id_final} 执行核心兑换步骤 (尝试 {attempt+1})"
                            )

                            # 1. 前置同步：拉人前确保人数状态绝对实时 (耗时操作)
                            # 使用独立 session，避免 sync 内部的 commit/rollback 污染主 session 状态
                            async with AsyncSessionLocal() as sync_session:
                                sync_result = await self.team_service.sync_team_info(
                                    team_id_final, sync_session
                                )
                            # sync_session 关闭时已自动提交或回滚，DB 状态已持久化

                            # 同步失败则直接中止本次迭代，避免继续使用失效的 Team
                            if not sync_result["success"]:
                                sync_error = sync_result.get("error", "Team 同步失败")
                                sync_error_text = str(sync_error).lower()
                                # 封禁/失效类错误需要排除该 team，下一轮换新的
                                if any(
                                    kw in sync_error_text
                                    for kw in ["封禁", "banned", "失效", "token_invalidated", "expired", "过期"]
                                ):
                                    current_target_team_id = None
                                    if team_id_final and team_id_final not in excluded_team_ids:
                                        excluded_team_ids.append(team_id_final)
                                raise Exception(f"Team 同步失败: {sync_error}")

                            # 2. 核心校验 (开启短事务)
                            if not retry_db.in_transaction():
                                await retry_db.begin()

                            try:
                                # 1. 验证和锁定码
                                stmt = (
                                    select(RedemptionCode)
                                    .where(RedemptionCode.code == code)
                                    .with_for_update()
                                )
                                res = await retry_db.execute(stmt)
                                rc = res.scalar_one_or_none()

                                if not rc:
                                    await retry_db.rollback()
                                    return {"success": False, "error": "兑换码不存在"}

                                if rc.status not in ["unused", "distributed", "warranty_active"]:
                                    if rc.status == "used":
                                        warranty_check = await self.warranty_service.validate_warranty_reuse(
                                            retry_db, code, email
                                        )
                                        if not warranty_check.get("can_reuse"):
                                            await retry_db.rollback()
                                            return {
                                                "success": False,
                                                "error": warranty_check.get("reason")
                                                or "兑换码已使用",
                                            }
                                    else:
                                        rc_status = rc.status  # rollback 前提取，避免 expire 后 lazy load
                                        await retry_db.rollback()
                                        return {
                                            "success": False,
                                            "error": f"兑换码状态无效: {rc_status}",
                                        }

                                # 2. 锁定并校验 Team
                                stmt = (
                                    select(Team)
                                    .where(Team.id == team_id_final)
                                    .with_for_update()
                                )
                                res = await retry_db.execute(stmt)
                                target_team = res.scalar_one_or_none()

                                if not target_team or target_team.status != "active":
                                    raise Exception(
                                        f"目标 Team {team_id_final} 不可用 ({target_team.status if target_team else 'None'})"
                                    )

                                if target_team.current_members >= target_team.max_members:
                                    target_team.status = "full"
                                    raise Exception("该 Team 已满, 请选择其他 Team 尝试")

                                # 提取必要信息后立即提交，释放 DB 锁以进行耗时的 API 调用
                                account_id_to_use = target_team.account_id
                                team_email_to_use = target_team.email
                                await retry_db.commit()
                            except Exception as e:
                                if retry_db.in_transaction():
                                    await retry_db.rollback()
                                raise e

                            # 3. 执行 API 邀请 (耗时操作，放事务外)
                            # 必须重新加载 target_team
                            res = await retry_db.execute(
                                select(Team).where(Team.id == team_id_final)
                            )
                            target_team = res.scalar_one_or_none()

                            access_token = await self.team_service.ensure_access_token(
                                target_team, retry_db
                            )
                            if not access_token:
                                raise Exception("获取 Team 访问权限失败，账户状态异常")

                            invite_res = await self.chatgpt_service.send_invite(
                                access_token,
                                account_id_to_use,
                                email,
                                retry_db,
                                identifier=team_email_to_use,
                            )

                            # 4. 后置处理与状态持久化 (第二次短事务)
                            if not retry_db.in_transaction():
                                await retry_db.begin()

                            try:
                                # 重新载入，确保状态最新
                                res = await retry_db.execute(
                                    select(RedemptionCode)
                                    .where(RedemptionCode.code == code)
                                    .with_for_update()
                                )
                                rc = res.scalar_one_or_none()
                                res = await retry_db.execute(
                                    select(Team)
                                    .where(Team.id == team_id_final)
                                    .with_for_update()
                                )
                                target_team = res.scalar_one_or_none()

                                if not invite_res["success"]:
                                    err = invite_res.get("error", "邀请失败")
                                    err_str = str(err).lower()
                                    if any(
                                        kw in err_str
                                        for kw in [
                                            "already in workspace",
                                            "already in team",
                                            "already a member",
                                        ]
                                    ):
                                        logger.info(
                                            f"用户 {email} 已经在 Team {team_id_final} 中，视为兑换成功"
                                        )
                                    else:
                                        if any(
                                            kw in err_str
                                            for kw in [
                                                "maximum number of seats",
                                                "full",
                                                "no seats",
                                            ]
                                        ):
                                            target_team.status = "full"
                                            await retry_db.commit()
                                            raise Exception(
                                                f"该 Team 席位已满 (API Error: {err})"
                                            )
                                        await retry_db.rollback()
                                        raise Exception(err)

                                # 成功逻辑
                                rc.status = "used"
                                rc.used_by_email = email
                                rc.used_team_id = team_id_final
                                rc.used_at = get_now()
                                if rc.has_warranty:
                                    days = rc.warranty_days or 30
                                    rc.warranty_expires_at = get_now() + timedelta(
                                        days=days
                                    )

                                record = RedemptionRecord(
                                    email=email,
                                    code=code,
                                    team_id=team_id_final,
                                    account_id=target_team.account_id,
                                    is_warranty_redemption=rc.has_warranty,
                                )
                                retry_db.add(record)
                                target_team.current_members += 1
                                if target_team.current_members >= target_team.max_members:
                                    target_team.status = "full"

                                await retry_db.commit()

                                # 核心步骤成功，准备返回结果
                                success_result = {
                                    "success": True,
                                    "message": "兑换成功！邀请链接已发送至您的邮箱，请及时查收。",
                                    "team_info": {
                                        "id": team_id_final,
                                        "team_name": target_team.team_name,
                                        "email": target_team.email,
                                        "expires_at": (
                                            target_team.expires_at.isoformat()
                                            if target_team.expires_at
                                            else None
                                        ),
                                    },
                                }
                                core_success = True
                            except Exception as e:
                                if retry_db.in_transaction():
                                    await retry_db.rollback()
                                raise e

                        # 如果核心步骤成功，跳出重试循环
                        if core_success:
                            break

                    except Exception as e:
                        last_error = str(e)
                        # 最后一次失败升为 ERROR + 完整堆栈；前几次降为 WARNING 避免日志噪音
                        if attempt == max_retries - 1:
                            logger.error(f"兑换迭代失败 ({attempt+1}): {last_error}")
                            logger.error(traceback.format_exc())
                        else:
                            logger.warning(f"兑换迭代失败 ({attempt+1}): {last_error}")
                            logger.debug(traceback.format_exc())

                        try:
                            if retry_db.in_transaction():
                                await retry_db.rollback()
                        except:
                            pass

                        # 判读是否中断重试
                        if any(
                            kw in last_error
                            for kw in ["不存在", "已使用", "已有正在使用", "质保已过期"]
                        ):
                            return {"success": False, "error": last_error}

                        # 判定是否需要换 Team（banned / 不可用），下一轮重新自动选
                        if "不可用" in last_error:
                            current_target_team_id = None
                            # 方案二：记录该 team，避免重试时因 DB 状态延迟再次选中同一个
                            if team_id_final and team_id_final not in excluded_team_ids:
                                excluded_team_ids.append(team_id_final)

                        # 判定是否需要永久标记为"满员"
                        if any(
                            kw in last_error.lower() for kw in ["已满", "seats", "full"]
                        ):
                            try:
                                if not team_id:
                                    from sqlalchemy import update as sqlalchemy_update

                                    await retry_db.execute(
                                        sqlalchemy_update(Team)
                                        .where(Team.id == team_id_final)
                                        .values(status="full")
                                    )
                                    await retry_db.commit()
                                current_target_team_id = None
                            except:
                                pass

                        if attempt < max_retries - 1:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue

                # async with retry_db 在此退出，session 自动关闭
                if core_success:
                    break

        if core_success:
            # 后台异步验证任务 (循环检测 3 次，确保 API 数据同步) - 移至后台以极大提高并发并防止 HTTP 超时
            asyncio.create_task(self._background_verify_sync(team_id_final, email))

            # 补货通知任务 (异步)
            asyncio.create_task(notification_service.check_and_notify_low_stock())

            return success_result
        else:
            return {
                "success": False,
                "error": f"兑换失败次数过多。最后报错: {last_error}",
            }

    async def _background_verify_sync(self, team_id: int, email: str):
        """
        后台异步验证并同步 (不阻塞 HTTP 请求)
        """
        async with AsyncSessionLocal() as db_session:
            try:
                is_verified = False
                for i in range(3):
                    await asyncio.sleep(5)
                    # 每次同步前确保 session 是最新的
                    sync_res = await self.team_service.sync_team_info(
                        team_id, db_session
                    )
                    member_emails = [
                        m.lower() for m in sync_res.get("member_emails", [])
                    ]
                    if email.lower() in member_emails:
                        is_verified = True
                        logger.info(
                            f"Team {team_id} [Background] 同步确认成功 (尝试第 {i+1} 次)"
                        )
                        break

                    if i < 2:
                        logger.warning(
                            f"Team {team_id} [Background] 尚未见到成员 {email}，准备第 {i+2} 次重试..."
                        )

                if not is_verified:
                    logger.error(
                        f"检测到“虚假成功”: Team {team_id} 兑换成功但 15s 后仍查不到成员 {email}"
                    )
                    # 在后台标记异常
                    stmt = select(Team).where(Team.id == team_id)
                    t_res = await db_session.execute(stmt)
                    target_t = t_res.scalar_one_or_none()
                    if target_t:
                        await self.team_service._handle_api_error(
                            {
                                "success": False,
                                "error": "兑换成功但 3 次同步均未见成员",
                                "error_code": "ghost_success",
                            },
                            target_t,
                            db_session,
                        )
            except Exception as e:
                logger.error(f"后台同步校验发生异常: {e}")


# 创建全局实例
redeem_flow_service = RedeemFlowService()
