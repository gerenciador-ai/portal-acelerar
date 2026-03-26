const axios = require('axios');
const fs = require('fs');
const path = require('path');

async function syncNibo() {
    console.log("🚀 Iniciando Sincronização Nibo (VMC Tech)...");
    const API_KEY_VMC = "BBC8B184DE0C41F8BF2EA9162263E72D";
    const CATEGORIA_ALVO = "311014001";
    const url = "https://api.nibo.com.br/empresas/v1/schedules?$top=1000";
    const hoje = new Date();
    try {
        const res = await axios.get(url, { headers: { 'apitoken': API_KEY_VMC, 'accept': 'application/json' } });
        const itens = res.data.items || [];
        const titulosAberto = itens.filter(item => {
            const catNome = item.category?.name || "";
            const vencimento = new Date(item.dueDate);
            return catNome.includes(CATEGORIA_ALVO) && item.openValue > 0 && vencimento < hoje;
        });
        let totalAberto = 0;
        const clientesUnicos = new Set();
        const mapaClientes = {};
        const mapaPiorFaixa = {};
        const contagemFaixas = { '0-30 dias': 0, '31-60 dias': 0, '61-90 dias': 0, '>90 dias': 0 };
        titulosAberto.forEach(item => {
            const valor = item.openValue;
            const clienteNome = item.stakeholder?.name || "N/A";
            const clienteId = item.stakeholder?.id || clienteNome;
            const vencimento = new Date(item.dueDate);
            const diffDays = Math.ceil(Math.abs(hoje - vencimento) / (1000 * 60 * 60 * 24));
            let faixa = '>90 dias';
            if (diffDays <= 30) faixa = '0-30 dias';
            else if (diffDays <= 60) faixa = '31-60 dias';
            else if (diffDays <= 90) faixa = '61-90 dias';
            totalAberto += valor;
            clientesUnicos.add(clienteId);
            if (!mapaClientes[clienteNome]) {
                mapaClientes[clienteNome] = { valorTotal: 0, parcelas: 0, piorFaixa: faixa, piorDias: diffDays };
            }
            mapaClientes[clienteNome].valorTotal += valor;
            mapaClientes[clienteNome].parcelas += 1;
            if (diffDays > mapaClientes[clienteNome].piorDias) {
                mapaClientes[clienteNome].piorFaixa = faixa;
                mapaClientes[clienteNome].piorDias = diffDays;
            }
            if (!mapaPiorFaixa[clienteId] || diffDays > mapaPiorFaixa[clienteId].dias) {
                mapaPiorFaixa[clienteId] = { faixa: faixa, dias: diffDays };
            }
        });
        Object.values(mapaPiorFaixa).forEach(v => contagemFaixas[v.faixa]++);
        const result = {
            updated_at: hoje.toISOString(),
            kpis: { total_aberto: totalAberto, clientes_inadimplentes: clientesUnicos.size, repasse_sittax: totalAberto * 0.30 },
            aging_chart: { labels: Object.keys(contagemFaixas), values: Object.values(contagemFaixas) },
            resumo_clientes: Object.entries(mapaClientes).sort((a, b) => b[1].parcelas - a[1].parcelas).map(([nome, d]) => ({ nome, valor: d.valorTotal, parcelas: d.parcelas, faixa: d.piorFaixa })),
            detalhamento_titulos: titulosAberto.sort((a, b) => new Date(a.dueDate) - new Date(b.dueDate)).map(item => {
                const venc = new Date(item.dueDate);
                const diff = Math.ceil(Math.abs(hoje - venc) / (1000 * 60 * 60 * 24));
                let f = diff > 90 ? '>90 dias' : (diff <= 30 ? '0-30 dias' : (diff <= 60 ? '31-60 dias' : '61-90 dias'));
                return { vencimento: venc.toISOString().split('T')[0], cliente: item.stakeholder?.name || 'N/A', valor: item.openValue, dias: diff, faixa: f };
            })
        };
        const dataDir = path.join(__dirname, '../data');
        if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);
        fs.writeFileSync(path.join(dataDir, 'inadimplencia.json'), JSON.stringify(result, null, 2));
        console.log("✅ Sincronização concluída com sucesso!");
    } catch (e) {
        console.error("❌ Erro na Sincronização:", e.message);
        process.exit(1);
    }
}
syncNibo();
