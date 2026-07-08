<script setup lang="ts">
import DataTable from '@/components/common/DataTable.vue'
import type { Column } from '@/components/common/types'
import type { OpsCodexAccountStatus } from '@/api/admin/ops'

const props = defineProps<{
  accounts: OpsCodexAccountStatus[]
  loading: boolean
}>()

const cols: Column[] = [
  { key: 'account_id', label: '#' },
  { key: 'name', label: '账号' },
  { key: 'schedulable', label: '状态' },
  { key: 'concurrency', label: '并发' },
  { key: 'success_count', label: '成功' },
  { key: 'error_429_count', label: '429' },
  { key: 'error_other_count', label: '其他错误' },
  { key: 'weekly_7d_pct', label: '周%' },
  { key: 'hourly_5h_pct', label: '5h%' },
  { key: 'weekly_7d_reset', label: '周重置' }
]
</script>

<template>
  <div class="flex h-full flex-col rounded-3xl bg-white p-6 shadow-sm ring-1 ring-gray-900/5 dark:bg-dark-800 dark:ring-dark-700">
    <h3 class="mb-4 text-sm font-bold text-gray-900 dark:text-white">Codex 监控 · 账号状态</h3>
    <DataTable
      :columns="cols"
      :data="props.accounts"
      :loading="props.loading"
      row-key="account_id"
      :estimate-row-height="48"
    >
      <template #cell-schedulable="{ value }">
        <span
          :class="[
            'rounded-full px-2 py-0.5 text-xs font-medium',
            value
              ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400'
              : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'
          ]"
        >
          {{ value ? '启用' : '禁用' }}
        </span>
      </template>
      <template #cell-weekly_7d_pct="{ value }">{{ value < 0 ? '—' : `${value}%` }}</template>
      <template #cell-hourly_5h_pct="{ value }">{{ value < 0 ? '—' : `${value}%` }}</template>
    </DataTable>
  </div>
</template>
