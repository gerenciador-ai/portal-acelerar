const axios = require('axios');
const fs = require('fs');
const path = require('path');

async function syncNibo() {
    console.log("🚀 Iniciando Sincronização Nibo de Alta Precisão (Modo Paginação)...");
    
    const CONFIG = [
        { name: 'vmctech', key: 'BBC8B184DE0C41F8BF2EA9162263E72D' },
        { name: 'victec', key: 'A967D3D9A45E4B8890F0437FEDCF6872' }
    ];
    
    const CATEGORIA_ALVO = "311014001";
    const hoje = new Date();
    const hojeZero = new Date(hoje.getFullYear(), hoje.getMonth(), hoje.getDate());
    const dataDir = path.join(process.cwd(), 'data');

    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

    for (const empresa of CONFIG) {
        console.log(`\n📦 Processando Empresa: ${empresa.name.toUpperCase()}...`);
        
        try {
            let todosItens = [];
            let skip = 0;
            const top = 500; // Buscar em blocos menores para evitar Erro 500
            let continua = true;

            while (continua) {
                console.log(`📡 Buscando bloco (skip: ${skip})...`);
                const url = `https://api.nibo.com.br/empresas/v1/schedules?$top=${top}&$skip=${skip}`;
                const res = await axios.get(url, {
                    headers: { 'apitoken': empresa.key, 'accept': 'application/json' },
                    timeout: 30000 // 30 segundos de timeout
                });

                const itens = res.data.items || [];
                todosItens = todosItens.concat(itens);
                
                console.log(`📥 Recebidos ${itens.length} itens neste bloco.`);
                
                if (itens.length < top) {
                    continua = false;
                } else {
                    skip += top;
                }
            }

            console.log(`✅ Total de itens recebidos: ${todosItens.length}`);
            
            const titulosAberto = todosItens.filter(item => {
                const catNome = item.category?.name || "";
                const vencimentoStr = item.dueDate;
                if (!vencimentoStr) return false;
                
                const vencimento = new Date(vencimentoStr);
                return catNome.includes(CATEGORIA_ALVO) && item.openValue > 0 && vencimento < hojeZero;
            });

            let totalAberto = 0;
            const clientesUnicos = new Set();
            const mapaClientes = {};
            const contagemFaixas = { '0-30 dias': 0, '31-60 dias': 0, '61-90 dias': 0, '>90 dias': 0 };

            titulosAberto.forEach(item => {
                const valor = item.openValue;
                const clienteNome = item.stakeholder?.name || "N/A";
                const clienteId = item.stakeholder?.id || clienteNome;
                const vencimento = new Date(item.dueDate);
                
                const diffTime = Math.abs(hojeZero - vencimento);
                const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                
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
            });

            Object.values(mapaClientes).forEach(c => {
                contagemFaixas[c.piorFaixa]++;
            });

            const result = {
                updated_at: hoje.toISOString(),
                kpis: {
                    total_aberto: totalAberto,
                    clientes_inadimplentes: clientesUnicos.size,
                    repasse_sittax: totalAberto * 0.30
                },
                aging_chart: {
                    labels: Object.keys(contagemFaixas),
                    values: Object.values(contagemFaixas)
                },
                resumo_clientes: Object.entries(mapaClientes)
                    .sort((a, b) => b[1].valorTotal - a[1].valorTotal)
                    .map(([nome, d]) => ({
                        nome,
                        valor: d.valorTotal,
                        parcelas: d.parcelas,
                        faixa: d.piorFaixa
                    })),
                detalhamento_titulos: titulosAberto
                    .sort((a, b) => new Date(a.dueDate) - new Date(b.dueDate))
                    .map(item => {
                        const venc = new Date(item.dueDate);
                        const diffTime = Math.abs(hojeZero - venc);
                        const diff = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                        let f = diff > 90 ? '>90 dias' : (diff <= 30 ? '0-30 dias' : (diff <= 60 ? '31-60 dias' : '61-90 dias'));
                        return {
                            vencimento: item.dueDate,
                            cliente: item.stakeholder?.name || 'N/A',
                            valor: item.openValue,
                            dias: diff,
                            faixa: f
                        };
                    })
            };

            const fileName = `inadimplencia_${empresa.name}.json`;
            const filePath = path.join(dataDir, fileName);
            fs.writeFileSync(filePath, JSON.stringify(result, null, 2));
            
            console.log(`✅ Sucesso: Gerado ${fileName} em ${filePath}`);
            console.log(`📊 Total Aberto: R$ ${totalAberto.toFixed(2)} | Clientes: ${clientesUnicos.size}`);

        } catch (e) {
            console.error(`❌ Erro na Empresa ${empresa.name}:`, e.message);
        }
    }
}

syncNibo();
