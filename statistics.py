class Statistics:
    def __init__(self):
        self.total_all_count = 0
        self.total_clean_count = 0
        self.total_nan_count = 0
        self.total_problematic_keys = 0
        self.df_stats = {}

    def update_df_stats(self, df_name, clean_count, all_count, nan_count):
        problematic_keys = all_count - clean_count - nan_count
        clean_percent = clean_count / all_count * 100 if all_count > 0 else 0

        self.df_stats[df_name] = {
            'clean_count': clean_count,
            'all_count': all_count,
            'nan_count': nan_count,
            'problematic_keys': problematic_keys,
            'clean_percent': clean_percent
        }

        self.total_all_count += all_count
        self.total_clean_count += clean_count
        self.total_nan_count += nan_count
        self.total_problematic_keys += problematic_keys

    def get_total_clean_percent(self):
        return self.total_clean_count / self.total_all_count * 100 if self.total_all_count > 0 else 0

    def get_output_stats(self, num_clean_keys, num_of_files, version):
        output_stats = {
            'num_clean_keys': num_clean_keys,
            'num_of_files': num_of_files,
            'version': version,
            'df_stats': self.df_stats,
            'total_all_count': self.total_all_count,
            'total_clean_count': self.total_clean_count,
            'total_nan_count': self.total_nan_count,
            'total_problematic_keys': self.total_problematic_keys,
            'total_clean_percent': self.get_total_clean_percent()
        }
        return output_stats
