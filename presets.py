# Market maker base configurations
MM_CONFIGS = {
    'mm_aggressive': {
        'base_spread_pct': 0.0004,
        'n_levels': 400,
        'base_size_outer': 4.0,
        'max_inventory': 10000,
        'skew_factor': 0.3,
        'repost_delay_ms': (100, 300),
        'ofi_sensitivity': 0.6,
        'cancel_on_shock': True,
        'shock_cooldown_ticks': 3,
        # lognormal size params — mean≈0.4 AEN, occasional spikes to 2-3 AEN
        # target: inner-20 total depth ≈ 75 AEN, single-side total ≈ 300-500 AEN
        'size_lognormal_mean': 0.0,   # exp(0.0) ≈ 1.0, after clip → avg ~0.4 AEN
        'size_lognormal_sigma': 0.9,
        'size_min': 0.1,
        'size_max': 3.0,
    },
    'mm_conservative': {
        'base_spread_pct': 0.0012,
        'n_levels': 400,
        'base_size_outer': 16.0,
        'max_inventory': 16000,
        'skew_factor': 0.2,
        'repost_delay_ms': (300, 700),
        'ofi_sensitivity': 0.7,
        'cancel_on_shock': False,
        'shock_cooldown_ticks': 3,
        # Conservative MM posts slightly larger sizes to provide backstop depth
        'size_lognormal_mean': 0.3,   # exp(0.3) ≈ 1.35, after clip → avg ~0.6 AEN
        'size_lognormal_sigma': 0.9,
        'size_min': 0.1,
        'size_max': 5.0,
    }
}


PRESETS = {
    'retail': {
        'name': '散户市场',
        'description': '流动性差的山寨币，大单容易推价',
        'mm_aggressive': {**MM_CONFIGS['mm_aggressive'],
                          'base_spread_pct': 0.003, 'n_levels': 15},
        'mm_conservative': {**MM_CONFIGS['mm_conservative'],
                            'base_spread_pct': 0.008, 'n_levels': 10},
        'noise_base_rate': 0.8,
        'arbitrageur_entry_threshold': 0.005,
        'oracle_volatility_mult': 1.5,
    },
    'mainstream': {
        'name': '主流市场（默认）',
        'description': '接近BTC/ETH的感觉，巨鲸冲击可见但会被吸收',
        'mm_aggressive': MM_CONFIGS['mm_aggressive'],
        'mm_conservative': MM_CONFIGS['mm_conservative'],
        'noise_base_rate': 1.0,
        'arbitrageur_entry_threshold': 0.0005,
        'oracle_volatility_mult': 1.0,
    },
    'extreme': {
        'name': '极端行情',
        'description': '体验市场崩盘或暴涨，强平潮，流动性危机',
        'mm_aggressive': {**MM_CONFIGS['mm_aggressive'],
                          'base_spread_pct': 0.008, 'n_levels': 12,
                          'repost_delay_ms': (500, 1500)},
        'mm_conservative': {**MM_CONFIGS['mm_conservative'],
                            'base_spread_pct': 0.02, 'n_levels': 8},
        'noise_base_rate': 2.5,
        'arbitrageur_entry_threshold': 0.005,
        'oracle_volatility_mult': 4.0,
        'oracle_initial_state': 'panic',
    },
}