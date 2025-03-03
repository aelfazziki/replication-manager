class CDCHandler:
    def __init__(self, task):
        self.task = task
        self.merger = BigQueryMerger(
            task.target.client, 
            task.target.dataset
        )
        
    def process_changes(self, changes):
        grouped_changes = self._group_changes_by_table(changes)
        
        for table, table_changes in grouped_changes.items():
            merge_keys = self.task.table_merge_keys.get(table, ['ROWID'])
            
            if self.task.merge_strategy['method'] == 'UPSERT':
                self.merger.execute_merge(
                    table, 
                    table_changes,
                    merge_keys
                )
            elif self.task.merge_strategy['method'] == 'HISTORY':
                self._handle_history_table(table, table_changes)

    def _group_changes_by_table(self, changes):
        # Group changes by table and operation type
        grouped = defaultdict(list)
        for change in changes:
            grouped[change['table']].append(change)
        return grouped