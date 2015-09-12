"vimrc specific for c++j
set tabstop=4
set softtabstop=4
set shiftwidth=4
set noexpandtab
set colorcolumn=110
highlight ColorColumn ctermbg=darkgray
let &path.="include,"
nnoremap <F4> :make!<cr>
