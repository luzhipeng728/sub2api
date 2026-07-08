<script setup lang="ts">
import type { OpsCodexOverviewResponse } from '@/api/admin/ops'

defineProps<{
  overview: OpsCodexOverviewResponse | null
  loading: boolean
}>()
</script>

<template>
  <div class="flex h-full flex-col rounded-3xl bg-white p-6 shadow-sm ring-1 ring-gray-900/5 dark:bg-dark-800 dark:ring-dark-700">
    <h3 class="mb-4 text-sm font-bold text-gray-900 dark:text-white">Codex 监控 · 概览</h3>

    <div class="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
      <div class="rounded-2xl bg-gray-50 p-4 dark:bg-dark-900">
        <div class="mb-1 text-xs text-gray-500 dark:text-gray-400">RPM(近1时均值)</div>
        <div class="font-mono text-xl font-bold text-gray-900 dark:text-white">
          {{ loading ? '—' : Math.round(overview?.rpm_1h ?? 0) }}
        </div>
      </div>
      <div class="rounded-2xl bg-gray-50 p-4 dark:bg-dark-900">
        <div class="mb-1 text-xs text-gray-500 dark:text-gray-400">总流量</div>
        <div class="font-mono text-xl font-bold text-gray-900 dark:text-white">
          {{ loading ? '—' : (overview?.total_traffic ?? 0) }}
        </div>
      </div>
      <div class="rounded-2xl bg-gray-50 p-4 dark:bg-dark-900">
        <div class="mb-1 text-xs text-gray-500 dark:text-gray-400">成功数</div>
        <div class="font-mono text-xl font-bold text-gray-900 dark:text-white">
          {{ loading ? '—' : (overview?.success_count ?? 0) }}
        </div>
      </div>
      <div class="rounded-2xl bg-gray-50 p-4 dark:bg-dark-900">
        <div class="mb-1 text-xs text-gray-500 dark:text-gray-400">使用账号数</div>
        <div class="font-mono text-xl font-bold text-gray-900 dark:text-white">
          {{ loading ? '—' : (overview?.accounts_used ?? 0) }}
        </div>
      </div>
      <div class="rounded-2xl bg-gray-50 p-4 dark:bg-dark-900">
        <div class="mb-1 text-xs text-gray-500 dark:text-gray-400">拦截数</div>
        <div class="font-mono text-xl font-bold text-amber-600 dark:text-amber-400">
          {{ loading ? '—' : (overview?.blocked_count ?? 0) }}
        </div>
      </div>
      <div class="rounded-2xl bg-gray-50 p-4 dark:bg-dark-900">
        <div class="mb-1 text-xs text-gray-500 dark:text-gray-400">429 数</div>
        <div class="font-mono text-xl font-bold text-amber-600 dark:text-amber-400">
          {{ loading ? '—' : (overview?.error_429_count ?? 0) }}
        </div>
      </div>
    </div>

    <table v-if="overview?.blocked_reasons?.length" class="mt-4 w-full border-collapse text-sm">
      <thead>
        <tr class="border-b border-gray-200 dark:border-dark-700">
          <th class="py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-dark-400">
            拦截原因
          </th>
          <th class="py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-dark-400">
            次数
          </th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="r in overview.blocked_reasons" :key="r.reason" class="border-b border-gray-100 dark:border-dark-800">
          <td class="py-2 text-gray-900 dark:text-gray-100">{{ r.reason }}</td>
          <td class="py-2 text-gray-900 dark:text-gray-100">{{ r.count }}</td>
        </tr>
      </tbody>
    </table>

    <div v-if="overview?.vistara" class="mt-4 flex flex-wrap gap-3 rounded-2xl bg-gray-50 p-4 dark:bg-dark-900">
      <div class="min-w-[110px] flex-1">
        <div class="text-[11px] text-gray-500 dark:text-gray-400">$/分</div>
        <div class="font-mono text-sm font-semibold text-gray-900 dark:text-white">
          {{ overview.vistara.usd_per_minute ?? '—' }}
        </div>
      </div>
      <div class="min-w-[110px] flex-1">
        <div class="text-[11px] text-gray-500 dark:text-gray-400">预估$/小时</div>
        <div class="font-mono text-sm font-semibold text-gray-900 dark:text-white">
          {{ overview.vistara.usd_per_hour ?? '—' }}
        </div>
      </div>
      <div class="min-w-[110px] flex-1">
        <div class="text-[11px] text-gray-500 dark:text-gray-400">预估$/天</div>
        <div class="font-mono text-sm font-semibold text-gray-900 dark:text-white">
          {{ overview.vistara.usd_per_day ?? '—' }}
        </div>
      </div>
      <div class="min-w-[110px] flex-1">
        <div class="text-[11px] text-gray-500 dark:text-gray-400">记录起累计$</div>
        <div class="font-mono text-sm font-semibold text-gray-900 dark:text-white">
          {{ overview.vistara.since_start_usd ?? '—' }}
        </div>
      </div>
      <div class="min-w-[110px] flex-1">
        <div class="text-[11px] text-gray-500 dark:text-gray-400">渠道总累计$</div>
        <div class="font-mono text-sm font-semibold text-gray-900 dark:text-white">
          {{ overview.vistara.total_usd ?? '—' }}
        </div>
      </div>
    </div>
  </div>
</template>
