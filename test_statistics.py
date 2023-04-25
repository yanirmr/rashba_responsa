from statistics import Statistics


def test_update_df_stats():
    stats = Statistics()
    stats.update_df_stats("df_1", 10, 20, 2)

    assert stats.total_all_count == 20
    assert stats.total_clean_count == 10
    assert stats.total_nan_count == 2
    assert stats.total_problematic_keys == 8
    assert stats.df_stats["df_1"] == {
        'clean_count': 10,
        'all_count': 20,
        'nan_count': 2,
        'problematic_keys': 8,
        'clean_percent': 50.0
    }


def test_get_total_clean_percent():
    stats = Statistics()
    stats.total_all_count = 50
    stats.total_clean_count = 35

    assert stats.get_total_clean_percent() == 70.0


def test_get_output_stats():
    stats = Statistics()
    stats.update_df_stats("df_1", 10, 20, 2)
    stats.update_df_stats("df_2", 15, 30, 3)

    output_stats = stats.get_output_stats(25, 2, "0.3.2")

    assert output_stats == {
        'num_clean_keys': 25,
        'num_of_files': 2,
        'version': "0.3.2",
        'df_stats': {
            'df_1': {
                'clean_count': 10,
                'all_count': 20,
                'nan_count': 2,
                'problematic_keys': 8,
                'clean_percent': 50.0
            },
            'df_2': {
                'clean_count': 15,
                'all_count': 30,
                'nan_count': 3,
                'problematic_keys': 12,
                'clean_percent': 50.0
            }
        },
        'total_all_count': 50,
        'total_clean_count': 25,
        'total_nan_count': 5,
        'total_problematic_keys': 20,
        'total_clean_percent': 50.0
    }
