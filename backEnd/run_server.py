#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_server.py — Walter 系统服务器启动入口

从 config/ 目录下的 YAML 文件读取站点和容器配置，
构建 ConfigurationService 与 ConfigurationClient，然后启动指定站点的服务器。

用法：
    # 启动站点 0
    python -m backEnd.run_server --site 0

    # 启动站点 1
    python -m backEnd.run_server --site 1

    # 或直接运行
    python backEnd/run_server.py --site 0
"""

import argparse
import os
import sys
import yaml

# 确保项目根目录在 sys.path 中，便于 import
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from backEnd.walter.configuration import (
    ConfigurationService,
    ConfigurationClient,
)


# ═══════════════════════════════════════════════════════════════════
#  YAML 加载
# ═══════════════════════════════════════════════════════════════════

def _resolve_config_path(filename: str) -> str:
    """将相对于 backEnd/config/ 目录的文件名解析为绝对路径"""
    config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")
    return os.path.join(config_dir, filename)


def load_site_config(path: str = None) -> dict:
    """
    加载 site.yaml 站点配置。

    Returns
    -------
    dict  包含 'lease_duration' 和 'sites' 列表
    """
    path = path or _resolve_config_path("site.yaml")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data


def load_user_config(path: str = None) -> dict:
    """
    加载 user.yaml 容器/对象配置。

    Returns
    -------
    dict  包含 'containers' 列表
    """
    path = path or _resolve_config_path("user.yaml")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data


# ═══════════════════════════════════════════════════════════════════
#  从 YAML 配置构建 Configuration Service & Clients
# ═══════════════════════════════════════════════════════════════════

def create_config_from_yaml(
    site_config: dict = None,
    user_config: dict = None,
) -> tuple:
    """
    从 YAML 配置构建 ConfigurationService 和所有站点的 ConfigurationClient。

    Parameters
    ----------
    site_config : site.yaml 的解析结果（dict），None 则自动加载
    user_config : user.yaml 的解析结果（dict），None 则自动加载

    Returns
    -------
    (config_service, clients_dict)
        config_service : ConfigurationService 实例
        clients_dict   : { site_id: ConfigurationClient, ... }
    """
    if site_config is None:
        site_config = load_site_config()
    if user_config is None:
        user_config = load_user_config()

    lease_duration = site_config.get("lease_duration", 60.0)
    sites = site_config.get("sites", [])
    containers = user_config.get("containers", [])

    # 1. 创建 ConfigurationService
    svc = ConfigurationService(lease_duration=lease_duration)

    # 2. 注册所有站点
    for site in sites:
        address = f"http://{site['host']}:{site['port']}"
        svc.register_site(site["id"], address)

    # 3. 注册所有容器
    for ctn in containers:
        svc.register_container(
            container_id=ctn["id"],
            preferred_site=ctn["preferred_site"],
            replica_sites=ctn["replica_sites"],
        )

    # 4. 为每个站点创建 ConfigurationClient
    clients = {}
    for site in sites:
        sid = site["id"]
        client = ConfigurationClient(site_id=sid, config_service=svc)
        clients[sid] = client

    # 5. 为每个站点自动获取其作为首选站点的容器租约
    #    按站点分组容器
    site_containers: dict = {}  # site_id -> set of container_ids
    for ctn in containers:
        pref = ctn["preferred_site"]
        site_containers.setdefault(pref, set()).add(ctn["id"])

    for sid, ctn_set in site_containers.items():
        if sid in clients:
            clients[sid].acquire_lease(ctn_set, duration=lease_duration)

    return svc, clients


# ═══════════════════════════════════════════════════════════════════
#  服务器启动
# ═══════════════════════════════════════════════════════════════════

def start_server(site_id: int,
                 site_config: dict = None,
                 user_config: dict = None,
                 debug: bool = True):
    """
    启动指定站点的 Walter 服务器。

    Parameters
    ----------
    site_id     : 要启动的站点 ID
    site_config : site.yaml 解析结果（可选）
    user_config : user.yaml 解析结果（可选）
    debug       : Flask debug 模式
    """
    if site_config is None:
        site_config = load_site_config()
    if user_config is None:
        user_config = load_user_config()

    # 构建配置
    svc, clients = create_config_from_yaml(site_config, user_config)

    if site_id not in clients:
        print(f"[ERROR] 站点 {site_id} 未在 site.yaml 中定义")
        sys.exit(1)

    client = clients[site_id]
    sites = site_config.get("sites", [])
    total_sites = len(sites)

    # 查找本站点的 host/port
    my_site = None
    for s in sites:
        if s["id"] == site_id:
            my_site = s
            break

    if my_site is None:
        print(f"[ERROR] 站点 {site_id} 未找到")
        sys.exit(1)

    host = my_site["host"]
    port = my_site["port"]
    name = my_site.get("name", f"server{site_id:02d}")

    # 构建 servers 列表（兼容 legacy 格式，供 Server/App 内部使用）
    servers_list = []
    for s in sites:
        addr = f"http://{s['host']}:{s['port']}"
        is_local = (s["id"] == site_id)
        servers_list.append([s.get("name", f"server{s['id']:02d}"),
                             s["id"], is_local, addr])

    # 构建 objects 字典（兼容 legacy 格式）
    containers = user_config.get("containers", [])
    objects_dict = {}
    for ctn in containers:
        objects_dict[ctn["id"]] = {"prefID": ctn["preferred_site"]}

    # 打印启动信息
    print("=" * 70)
    print(f"  Walter Server — 站点 {site_id} ({name})")
    print(f"  监听: {host}:{port}")
    print(f"  总站点数: {total_sites}")
    print("-" * 70)
    print(f"  配置服务: {svc}")
    print(f"  本站客户端: {client}")
    print(f"  服务器列表: {servers_list}")
    print(f"  对象列表: {objects_dict}")
    print("=" * 70)

    # TODO: 这里将来替换为正式的 Server + App 初始化
    # 目前打印配置验证信息
    #
    # 示例（待 Server/App 完善后启用）：
    #   from backEnd.walter.server import Server
    #   from backEnd.app import Backend
    #
    #   db_server = Server(total_server_num=total_sites)
    #   backend = Backend(name)
    #   app = backend.create_app(host, port, name, db_server)
    #   app.run(host, port, debug=debug)

    print("\n[INFO] 配置加载完成。服务器逻辑待完善后可在此处启动 Flask app。")
    print("[INFO] 当前仅验证配置是否正确加载。\n")

    # 输出配置快照
    import json
    print("[ConfigurationService snapshot]")
    print(json.dumps(svc.snapshot(), indent=2, default=str))
    print()
    print(f"[ConfigurationClient site={site_id} cache]")
    print(json.dumps(client.cache_snapshot(), indent=2, default=str))


# ═══════════════════════════════════════════════════════════════════
#  命令行入口
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Walter 系统服务器启动入口"
    )
    parser.add_argument(
        "--site", type=int, required=True,
        help="要启动的站点 ID（对应 site.yaml 中的 id）"
    )
    parser.add_argument(
        "--site-config", type=str, default=None,
        help="site.yaml 文件路径（默认: backEnd/config/site.yaml）"
    )
    parser.add_argument(
        "--user-config", type=str, default=None,
        help="user.yaml 文件路径（默认: backEnd/config/user.yaml）"
    )
    parser.add_argument(
        "--no-debug", action="store_true",
        help="关闭 Flask debug 模式"
    )
    args = parser.parse_args()

    site_cfg = load_site_config(args.site_config) if args.site_config else None
    user_cfg = load_user_config(args.user_config) if args.user_config else None

    start_server(
        site_id=args.site,
        site_config=site_cfg,
        user_config=user_cfg,
        debug=not args.no_debug,
    )


if __name__ == "__main__":
    main()
